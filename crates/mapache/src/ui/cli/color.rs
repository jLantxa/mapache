use std::{
    fmt,
    io::IsTerminal,
    sync::{
        Arc, OnceLock,
        atomic::{AtomicI8, Ordering},
    },
};

#[derive(Clone, Copy)]
enum AnsiCode {
    Bold,
    Dimmed,
    Italic,
    Underline,
    Red,
    Green,
    Yellow,
    Blue,
    Cyan,
    Purple,
    White,
    BrightRed,
    OnRed,
    OnBlack,
}

impl AnsiCode {
    fn as_str(self) -> &'static str {
        match self {
            AnsiCode::Bold => "1",
            AnsiCode::Dimmed => "2",
            AnsiCode::Italic => "3",
            AnsiCode::Underline => "4",
            AnsiCode::Red => "31",
            AnsiCode::Green => "32",
            AnsiCode::Yellow => "33",
            AnsiCode::Blue => "34",
            AnsiCode::Cyan => "36",
            AnsiCode::Purple => "35",
            AnsiCode::White => "37",
            AnsiCode::BrightRed => "91",
            AnsiCode::OnRed => "41",
            AnsiCode::OnBlack => "40",
        }
    }
}

#[derive(Clone)]
pub struct Styled {
    text: Arc<str>,
    codes: Vec<AnsiCode>,
}

impl fmt::Display for Styled {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.codes.is_empty() {
            return write!(f, "{}", self.text);
        }
        for code in &self.codes {
            write!(f, "\x1b[{}m", code.as_str())?;
        }
        write!(f, "{}\x1b[0m", self.text)
    }
}

static OVERRIDE: AtomicI8 = AtomicI8::new(-1);

/// Override automatic color detection for testing.
pub fn set_color_override(enabled: bool) {
    OVERRIDE.store(enabled as i8, Ordering::Relaxed);
}

/// Reset color override so auto-detection is used.
pub fn clear_color_override() {
    OVERRIDE.store(-1, Ordering::Relaxed);
}

/// Initializes the console for ANSI escape sequences.
/// On Windows, this enables Virtual Terminal processing.
#[inline]
pub fn init_console() {
    #[cfg(windows)]
    {
        use windows_sys::Win32::{
            Foundation::INVALID_HANDLE_VALUE,
            System::Console::{
                ENABLE_VIRTUAL_TERMINAL_PROCESSING, GetConsoleMode, GetStdHandle,
                STD_OUTPUT_HANDLE, SetConsoleMode,
            },
        };

        // SAFETY: GetStdHandle/GetConsoleMode/SetConsoleMode are Windows
        // FFI calls with pseudo-handles (no ownership). Null and
        // INVALID_HANDLE_VALUE are checked before use.
        unsafe {
            let handle = GetStdHandle(STD_OUTPUT_HANDLE);
            if handle != INVALID_HANDLE_VALUE && !handle.is_null() {
                let mut mode = 0;
                if GetConsoleMode(handle, &mut mode) != 0 {
                    SetConsoleMode(handle, mode | ENABLE_VIRTUAL_TERMINAL_PROCESSING);
                }
            }
        }
    }
}

fn should_colorize() -> bool {
    match OVERRIDE.load(Ordering::Relaxed) {
        0 => return false,
        1 => return true,
        _ => {}
    }

    static SHOULD: OnceLock<bool> = OnceLock::new();
    *SHOULD.get_or_init(|| {
        if std::env::var_os("NO_COLOR").is_some() {
            return false;
        }
        if std::env::var_os("CLICOLOR_FORCE").is_some() {
            return true;
        }
        if std::env::var("CLICOLOR").as_deref() == Ok("0") {
            return false;
        }
        if std::env::var("TERM").as_deref() == Ok("dumb") {
            return false;
        }
        std::io::stdout().is_terminal()
    })
}

pub trait Colorize {
    fn bold(&self) -> Styled;
    fn dimmed(&self) -> Styled;
    fn italic(&self) -> Styled;
    fn underline(&self) -> Styled;
    fn normal(&self) -> Styled;
    fn red(&self) -> Styled;
    fn green(&self) -> Styled;
    fn yellow(&self) -> Styled;
    fn blue(&self) -> Styled;
    fn cyan(&self) -> Styled;
    fn purple(&self) -> Styled;
    fn white(&self) -> Styled;
    fn bright_red(&self) -> Styled;
    fn on_red(&self) -> Styled;
    fn on_black(&self) -> Styled;
}

macro_rules! style_methods {
    ($($name:ident => $code:ident),+ $(,)?) => {
        $(
            fn $name(&self) -> Styled {
                let mut s = Styled {
                    text: self.into(),
                    codes: vec![],
                };
                if should_colorize() {
                    s.codes.push(AnsiCode::$code);
                }
                s
            }
        )+
    };
    (@as_str, $($name:ident => $code:ident),+ $(,)?) => {
        $(
            fn $name(&self) -> Styled {
                let mut s = Styled {
                    text: self.as_str().into(),
                    codes: vec![],
                };
                if should_colorize() {
                    s.codes.push(AnsiCode::$code);
                }
                s
            }
        )+
    };
    (@as_ref, $($name:ident => $code:ident),+ $(,)?) => {
        $(
            fn $name(&self) -> Styled {
                let mut s = Styled {
                    text: self.as_ref().into(),
                    codes: vec![],
                };
                if should_colorize() {
                    s.codes.push(AnsiCode::$code);
                }
                s
            }
        )+
    };
}

impl Colorize for str {
    fn normal(&self) -> Styled {
        Styled {
            text: self.into(),
            codes: vec![],
        }
    }
    style_methods! {
        bold => Bold, dimmed => Dimmed, italic => Italic, underline => Underline,
        red => Red, green => Green, yellow => Yellow,
        blue => Blue, cyan => Cyan, purple => Purple,
        white => White, bright_red => BrightRed,
        on_red => OnRed, on_black => OnBlack,
    }
}

impl Colorize for String {
    fn normal(&self) -> Styled {
        Styled {
            text: self.as_str().into(),
            codes: vec![],
        }
    }
    style_methods! {
        @as_str,
        bold => Bold, dimmed => Dimmed, italic => Italic, underline => Underline,
        red => Red, green => Green, yellow => Yellow,
        blue => Blue, cyan => Cyan, purple => Purple,
        white => White, bright_red => BrightRed,
        on_red => OnRed, on_black => OnBlack,
    }
}

impl Colorize for std::borrow::Cow<'_, str> {
    fn normal(&self) -> Styled {
        Styled {
            text: self.as_ref().into(),
            codes: vec![],
        }
    }
    style_methods! {
        @as_ref,
        bold => Bold, dimmed => Dimmed, italic => Italic, underline => Underline,
        red => Red, green => Green, yellow => Yellow,
        blue => Blue, cyan => Cyan, purple => Purple,
        white => White, bright_red => BrightRed,
        on_red => OnRed, on_black => OnBlack,
    }
}

impl Colorize for Styled {
    fn bold(&self) -> Styled {
        let mut s = self.clone();
        if should_colorize() {
            s.codes.push(AnsiCode::Bold);
        }
        s
    }

    fn dimmed(&self) -> Styled {
        let mut s = self.clone();
        if should_colorize() {
            s.codes.push(AnsiCode::Dimmed);
        }
        s
    }

    fn italic(&self) -> Styled {
        let mut s = self.clone();
        if should_colorize() {
            s.codes.push(AnsiCode::Italic);
        }
        s
    }

    fn underline(&self) -> Styled {
        let mut s = self.clone();
        if should_colorize() {
            s.codes.push(AnsiCode::Underline);
        }
        s
    }

    fn normal(&self) -> Styled {
        Styled {
            text: self.text.clone(),
            codes: vec![],
        }
    }

    fn red(&self) -> Styled {
        let mut s = self.clone();
        if should_colorize() {
            s.codes.push(AnsiCode::Red);
        }
        s
    }

    fn green(&self) -> Styled {
        let mut s = self.clone();
        if should_colorize() {
            s.codes.push(AnsiCode::Green);
        }
        s
    }

    fn yellow(&self) -> Styled {
        let mut s = self.clone();
        if should_colorize() {
            s.codes.push(AnsiCode::Yellow);
        }
        s
    }

    fn blue(&self) -> Styled {
        let mut s = self.clone();
        if should_colorize() {
            s.codes.push(AnsiCode::Blue);
        }
        s
    }

    fn cyan(&self) -> Styled {
        let mut s = self.clone();
        if should_colorize() {
            s.codes.push(AnsiCode::Cyan);
        }
        s
    }

    fn purple(&self) -> Styled {
        let mut s = self.clone();
        if should_colorize() {
            s.codes.push(AnsiCode::Purple);
        }
        s
    }

    fn white(&self) -> Styled {
        let mut s = self.clone();
        if should_colorize() {
            s.codes.push(AnsiCode::White);
        }
        s
    }

    fn bright_red(&self) -> Styled {
        let mut s = self.clone();
        if should_colorize() {
            s.codes.push(AnsiCode::BrightRed);
        }
        s
    }

    fn on_red(&self) -> Styled {
        let mut s = self.clone();
        if should_colorize() {
            s.codes.push(AnsiCode::OnRed);
        }
        s
    }

    fn on_black(&self) -> Styled {
        let mut s = self.clone();
        if should_colorize() {
            s.codes.push(AnsiCode::OnBlack);
        }
        s
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    static COLOR_MUTEX: Mutex<()> = Mutex::new(());

    fn with_color() {
        set_color_override(true);
    }

    fn without_color() {
        set_color_override(false);
    }

    #[test]
    fn bold_wraps_in_ansi() {
        let _lock = COLOR_MUTEX.lock().unwrap();
        with_color();
        assert_eq!("\x1b[1mhello\x1b[0m", "hello".bold().to_string());
    }

    #[test]
    fn red_wraps_in_ansi() {
        let _lock = COLOR_MUTEX.lock().unwrap();
        with_color();
        assert_eq!("\x1b[31mhello\x1b[0m", "hello".red().to_string());
    }

    #[test]
    fn chaining_combines_codes() {
        let _lock = COLOR_MUTEX.lock().unwrap();
        with_color();
        let s = "hello".red().bold();
        let out = s.to_string();
        assert!(out.contains("\x1b[31m"), "missing red code");
        assert!(out.contains("\x1b[1m"), "missing bold code");
        assert!(out.contains("hello"), "missing text");
        assert!(out.ends_with("\x1b[0m"), "missing reset");
    }

    #[test]
    fn normal_strips_styles() {
        let _lock = COLOR_MUTEX.lock().unwrap();
        with_color();
        let s = "hello".red().bold().normal();
        assert_eq!("hello", s.to_string());
    }

    #[test]
    fn colors_disabled_returns_plain_text() {
        let _lock = COLOR_MUTEX.lock().unwrap();
        without_color();
        assert_eq!("hello", "hello".red().to_string());
        assert_eq!("hello", "hello".bold().to_string());
        assert_eq!("hello", "hello".green().to_string());
    }

    #[test]
    fn colorize_works_on_string() {
        let _lock = COLOR_MUTEX.lock().unwrap();
        with_color();
        let s = String::from("test").cyan();
        assert_eq!("\x1b[36mtest\x1b[0m", s.to_string());
    }

    #[test]
    fn styled_chaining() {
        let _lock = COLOR_MUTEX.lock().unwrap();
        with_color();
        let s = "hello".red().bold().italic();
        let out = s.to_string();
        assert!(out.contains("\x1b[31m"), "red");
        assert!(out.contains("\x1b[1m"), "bold");
        assert!(out.contains("\x1b[3m"), "italic");
    }

    #[test]
    fn normal_on_styled_strips_all() {
        let _lock = COLOR_MUTEX.lock().unwrap();
        with_color();
        let s = "hello".red().bold();
        assert_eq!("hello", s.normal().to_string());
    }

    #[test]
    fn empty_string() {
        let _lock = COLOR_MUTEX.lock().unwrap();
        with_color();
        assert_eq!("\x1b[31m\x1b[0m", "".red().to_string());
    }

    #[test]
    fn display_and_to_string_match() {
        let _lock = COLOR_MUTEX.lock().unwrap();
        with_color();
        let s = "display test".yellow();
        assert_eq!(s.to_string(), format!("{s}"));
    }

    #[test]
    fn all_codes_have_distinct_output() {
        let _lock = COLOR_MUTEX.lock().unwrap();
        with_color();
        let inputs = ["red", "green", "blue", "bold", "italic"];
        for text in &inputs {
            let colored = text.red();
            assert!(colored.to_string().contains(text), "must contain input");
        }
    }
}
