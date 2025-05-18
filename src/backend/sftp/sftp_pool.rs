// [backup] is an incremental backup tool
// Copyright (C) 2025  Javier Lancha Vázquez <javier.lancha@gmail.com>
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

use anyhow::{Context, Result};
use ssh2::{Session, Sftp};
use std::collections::VecDeque;
use std::net::TcpStream;
use std::path::Path;
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use crate::cli;

/// Represents an SFTP connection stored within the pool, including metadata for management.
struct PooledConnection {
    session: Session,
    sftp: Sftp,
    last_used: Instant, // To manage idle timeout
}

/// The mutable internal state of the pool, protected by a Mutex.
struct PoolState {
    connections: VecDeque<PooledConnection>, // Connections available in the pool
    num_established: usize, // Total number of active connections (in pool + borrowed)
}

/// An SFTP connection pool that manages a limited number of concurrent SFTP sessions.
///
/// This pool allows multiple threads to concurrently request and use SFTP connections.
/// It handles connection creation, pooling, waiting when the pool is full,
/// and basic idle timeout management.
pub struct SftpConnectionPool {
    addr: String,
    username: String,
    password: String,
    max_connections: usize,
    connection_timeout: Duration, // Timeout for waiting to get a connection
    idle_timeout: Duration,       // Time after which an idle connection is closed
    state_and_cvar: Arc<(Mutex<PoolState>, Condvar)>,
}

impl SftpConnectionPool {
    /// Creates a new SFTP connection pool.
    ///
    /// # Arguments
    /// * `addr` - The address of the SFTP server (e.g., "sftp.example.com:22").
    /// * `username` - The username for the SFTP connection.
    /// * `password` - The password for the SFTP connection.
    /// * `max_connections` - The maximum number of connections the pool will maintain.
    /// * `connection_timeout_seconds` - The maximum time in seconds a thread will wait to get a connection.
    /// * `idle_timeout_seconds` - The time in seconds after which an idle connection will be closed.
    pub fn new(
        addr: String,
        username: String,
        password: String,
        max_connections: usize,
        connection_timeout_seconds: u64,
        idle_timeout_seconds: u64,
    ) -> Self {
        SftpConnectionPool {
            addr,
            username,
            password,
            max_connections,
            connection_timeout: Duration::from_secs(connection_timeout_seconds),
            idle_timeout: Duration::from_secs(idle_timeout_seconds),
            state_and_cvar: Arc::new((
                Mutex::new(PoolState {
                    connections: VecDeque::new(),
                    num_established: 0,
                }),
                Condvar::new(),
            )),
        }
    }

    /// Attempts to get an SFTP connection from the pool.
    ///
    /// This method will block if the pool is currently at `max_connections`
    /// and no connections are available for reuse, waiting up to `connection_timeout`.
    ///
    /// Returns an `SftpSessionClient` which manages the automatic return of the connection
    /// to the pool when it goes out of scope.
    pub fn get(&self) -> Result<SftpSessionClient> {
        let (lock, cvar) = &*self.state_and_cvar;
        let mut state_guard = lock.lock().unwrap();

        loop {
            while let Some(mut pooled_conn) = state_guard.connections.pop_front() {
                // If the connection has been idle for too long, close it
                if pooled_conn.last_used.elapsed() > self.idle_timeout {
                    state_guard.num_established -= 1;
                    continue;
                }

                // Perform a lightweight check (ping) to see if the connection is still alive.
                // This adds latency but helps avoid using dead connections.
                // An alternative is to skip this and rely on the actual operation failing.
                if pooled_conn.last_used.elapsed() > Duration::from_secs(5) {
                    if let Err(e) = pooled_conn.sftp.stat(Path::new(".")) {
                        cli::log_warning(&format!(
                            "Pooled connection health check failed: {}. Closing and trying next.",
                            e
                        ));
                        state_guard.num_established -= 1;
                        continue;
                    }
                }

                pooled_conn.last_used = Instant::now();
                return Ok(SftpSessionClient {
                    session: pooled_conn.session,
                    sftp: pooled_conn.sftp,
                    pool: Arc::new(self.clone()),
                });
            }

            if state_guard.num_established < self.max_connections {
                drop(state_guard);

                let (session, sftp) =
                    match create_new_sftp_connection(&self.addr, &self.username, &self.password) {
                        Ok(conn) => conn,
                        Err(e) => {
                            return Err(e)
                                .with_context(|| "Failed to create new SFTP connection for pool");
                        }
                    };

                let (lock, _) = &*self.state_and_cvar;
                let mut state_guard = lock.lock().unwrap();
                state_guard.num_established += 1;

                return Ok(SftpSessionClient {
                    session,
                    sftp,
                    pool: Arc::new(self.clone()),
                });
            }

            let (new_guard, wait_timeout_result) = cvar
                .wait_timeout(state_guard, self.connection_timeout)
                .unwrap();
            state_guard = new_guard;

            if wait_timeout_result.timed_out() {
                return Err(anyhow::anyhow!(
                    "Timeout waiting for SFTP connection from pool"
                ));
            }
        }
    }

    /// Internal method to return a connection to the pool.
    /// Called by the `Drop` implementation of `SftpSessionClient`.
    fn put(&self, session: Session, sftp: Sftp) {
        let (lock, cvar) = &*self.state_and_cvar;
        let mut state_guard = lock.lock().unwrap();

        // Before returning, check if the connection has been idle for too long
        // or if adding it back would exceed the effective pool size (though get() prevents this).
        // The `num_established` count is already decremented in Drop, so we just push back
        // if there's space in the queue.
        if state_guard.connections.len() < self.max_connections {
            state_guard.connections.push_back(PooledConnection {
                session,
                sftp,
                last_used: Instant::now(),
            });

            // Notify a waiting thread that a connection is available
            cvar.notify_one();
        } else {
            state_guard.num_established -= 1;
        }
    }
}

// Implement Clone for SftpConnectionPool so Arc<SftpConnectionPool> can be cloned
// and passed to SftpSessionClient. This only clones the Arc, not the pool data.
impl Clone for SftpConnectionPool {
    fn clone(&self) -> Self {
        SftpConnectionPool {
            addr: self.addr.clone(),
            username: self.username.clone(),
            password: self.password.clone(),
            max_connections: self.max_connections,
            connection_timeout: self.connection_timeout,
            idle_timeout: self.idle_timeout,
            state_and_cvar: Arc::clone(&self.state_and_cvar), // Clone the Arc containing Mutex and Condvar
        }
    }
}

/// Represents an SFTP connection borrowed from the pool for a work session.
///
/// When this struct goes out of scope, the underlying connection is automatically
/// returned to the pool via its `Drop` implementation.
pub struct SftpSessionClient {
    session: Session,
    sftp: Sftp,
    pool: Arc<SftpConnectionPool>, // Reference back to the pool
}

impl SftpSessionClient {
    /// Gets an immutable reference to the `Sftp` object to perform operations.
    pub fn sftp(&self) -> &Sftp {
        &self.sftp
    }

    /// Gets an immutable reference to the `Session` object.
    pub fn session(&self) -> &Session {
        &self.session
    }
}

/// Implements the `Drop` trait for `SftpSessionClient` to automatically return
/// the connection to the pool when the client goes out of scope.
impl Drop for SftpSessionClient {
    fn drop(&mut self) {
        // Return the connection to the pool.
        // Use std::mem::replace to move ownership of the fields out of self.
        // The dummy values created by Session::new() and Sftp::new() are immediately
        // dropped after the original values are moved into pool.put().
        let session = std::mem::replace(&mut self.session, Session::new().unwrap());
        // Note: Sftp::new requires a Session, so we use the dummy session created above.
        let sftp = std::mem::replace(&mut self.sftp, session.sftp().unwrap());

        self.pool.put(session, sftp);
    }
}

/// Helper function to create a new SSH/SFTP connection.
/// This function is blocking.
fn create_new_sftp_connection(
    addr: &str,
    username: &str,
    password: &str,
) -> Result<(Session, Sftp)> {
    let tcp = TcpStream::connect(addr).with_context(|| "Failed to connect to SFTP server")?;
    let mut session = Session::new().with_context(|| "Failed to create SSH session")?;
    session.set_tcp_stream(tcp);
    session
        .handshake()
        .with_context(|| "Failed to perform SSH handshake")?;
    session
        .userauth_password(&username, &password)
        .with_context(|| "Failed to authenticate with password")?;

    session.set_keepalive(true, 30);

    let sftp = session
        .sftp()
        .with_context(|| "Failed to create SFTP session")?;

    Ok((session, sftp))
}

// Example Usage (requires a running SFTP server for testing)
#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;
    use std::thread;
    use std::time::Duration;

    // NOTE: This test requires a running SFTP server at the specified address
    // with the given username and password. Replace with your test credentials.
    const TEST_SFTP_ADDR: &str = "host:22";
    const TEST_SFTP_USER: &str = "user";
    const TEST_SFTP_PASS: &str = "password";

    #[test]
    #[ignore = "requires SFTP credentials"]
    fn test_pool_basic_get_put() -> Result<()> {
        // Create a pool with 2 max connections, 5s conn timeout, 10s idle timeout
        let pool = SftpConnectionPool::new(
            TEST_SFTP_ADDR.to_string(),
            TEST_SFTP_USER.to_string(),
            TEST_SFTP_PASS.to_string(),
            2,  // max_connections
            5,  // connection_timeout_seconds
            10, // idle_timeout_seconds
        );

        // Get a connection
        println!("Test 1: Getting first connection...");
        let client1 = pool.get()?;
        println!("Test 1: Got first connection.");

        // Perform a simple operation
        let sftp1 = client1.sftp();
        sftp1
            .stat(std::path::Path::new("."))
            .with_context(|| "Stat failed on client1")?;
        println!("Test 1: Stat successful on client1.");

        // client1 goes out of scope here and is dropped, returning the connection to the pool.
        drop(client1);
        println!("Test 1: Client1 dropped, connection returned to pool.");

        // Get another connection (should reuse the one from client1)
        println!("Test 1: Getting second connection...");
        let client2 = pool.get()?;
        println!("Test 1: Got second connection.");

        let sftp2 = client2.sftp();
        sftp2
            .stat(std::path::Path::new("."))
            .with_context(|| "Stat failed on client2")?;
        println!("Test 1: Stat successful on client2.");

        // client2 goes out of scope here
        drop(client2);
        println!("Test 1: Client2 dropped, connection returned to pool.");

        Ok(())
    }

    #[test]
    #[ignore = "requires SFTP credentials"]
    fn test_pool_concurrency() -> Result<()> {
        // Create a pool with 2 max connections
        let pool = Arc::new(SftpConnectionPool::new(
            TEST_SFTP_ADDR.to_string(),
            TEST_SFTP_USER.to_string(),
            TEST_SFTP_PASS.to_string(),
            2,  // max_connections
            5,  // connection_timeout_seconds
            10, // idle_timeout_seconds
        ));

        let mut handles = vec![];

        // Spawn 3 threads, but the pool only allows 2 concurrent connections
        for i in 0..3 {
            let pool_clone = Arc::clone(&pool);
            let handle = thread::spawn(move || {
                println!("Thread {} trying to get connection...", i);
                let client_res = pool_clone.get();
                match client_res {
                    Ok(client) => {
                        println!("Thread {} got connection.", i);
                        // Perform some work that takes time
                        thread::sleep(Duration::from_secs(2));
                        let sftp = client.sftp();
                        if let Err(e) = sftp.stat(std::path::Path::new(".")) {
                            eprintln!("Thread {} stat failed: {}", i, e);
                        } else {
                            println!("Thread {} stat successful.", i);
                        }
                        // client is dropped here, returning connection to pool
                        println!("Thread {} dropping connection.", i);
                    }
                    Err(e) => {
                        eprintln!("Thread {} failed to get connection: {}", i, e);
                    }
                }
            });
            handles.push(handle);
        }

        // Wait for all threads to complete
        for handle in handles {
            handle.join().expect("Thread panicked");
        }

        println!("Test 2: Concurrency test finished.");

        Ok(())
    }

    #[test]
    #[ignore = "requires SFTP credentials"]
    fn test_pool_timeout() -> Result<()> {
        // Create a pool with 1 max connection and a short connection timeout
        let pool = SftpConnectionPool::new(
            TEST_SFTP_ADDR.to_string(),
            TEST_SFTP_USER.to_string(),
            TEST_SFTP_PASS.to_string(),
            1,  // max_connections
            1,  // connection_timeout_seconds (short timeout)
            10, // idle_timeout_seconds
        );

        // Get the only connection
        println!("Test 3: Getting first connection...");
        let client1 = pool.get()?;
        println!("Test 3: Got first connection.");

        // In another thread, try to get a connection immediately (should time out)
        let pool_clone = Arc::new(pool.clone());
        let handle = thread::spawn(move || {
            println!("Thread trying to get connection (should timeout)...");
            let client_res = pool_clone.get();
            match client_res {
                Ok(_) => {
                    eprintln!("Thread unexpectedly got a connection!");
                }
                Err(e) => {
                    println!("Thread failed to get connection as expected: {}", e);
                }
            }
        });

        // Wait for the timeout thread to finish
        handle.join().expect("Timeout thread panicked");

        // client1 goes out of scope here
        drop(client1);
        println!("Test 3: Client1 dropped, connection returned to pool.");

        // Now we should be able to get a connection again
        println!("Test 3: Getting connection after timeout thread...");
        let client2 = pool.get()?;
        println!("Test 3: Got connection after timeout thread.");
        drop(client2);

        println!("Test 3: Timeout test finished.");

        Ok(())
    }

    #[test]
    #[ignore = "requires SFTP credentials"]
    fn test_pool_idle_timeout() -> Result<()> {
        // Create a pool with 1 max connection and a short idle timeout
        let pool = SftpConnectionPool::new(
            TEST_SFTP_ADDR.to_string(),
            TEST_SFTP_USER.to_string(),
            TEST_SFTP_PASS.to_string(),
            1, // max_connections
            5, // connection_timeout_seconds
            1, // idle_timeout_seconds (short idle timeout)
        );

        // Get the connection
        println!("Test 4: Getting connection...");
        let client1 = pool.get()?;
        println!("Test 4: Got connection.");

        // Drop the connection immediately
        drop(client1);
        println!("Test 4: Client1 dropped, connection returned to pool.");

        // Wait longer than the idle timeout
        println!("Test 4: Waiting for idle timeout...");
        thread::sleep(Duration::from_secs(3)); // Wait > 1 second idle timeout

        // Get a connection again - the previous one should be discarded due to idle timeout
        println!("Test 4: Getting connection again (should create new)...");
        let client2 = pool.get()?;
        println!("Test 4: Got connection again.");

        // Perform a simple operation to ensure it's a live connection
        let sftp2 = client2.sftp();
        sftp2
            .stat(std::path::Path::new("."))
            .with_context(|| "Stat failed on client2 after idle timeout")?;
        println!("Test 4: Stat successful on client2.");

        drop(client2);
        println!("Test 4: Client2 dropped.");

        println!("Test 4: Idle timeout test finished.");

        Ok(())
    }
}
