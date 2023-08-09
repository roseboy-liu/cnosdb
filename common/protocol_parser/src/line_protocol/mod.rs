use bumpalo::Bump;

use crate::line_protocol::parser::Parser;
use crate::{Line, Result};

pub mod parser;

pub fn line_protocol_to_lines<'a>(
    lines: &'a str,
    default_time: i64,
    arena: &'a Bump,
) -> Result<Vec<Line<'a>, &'a Bump>> {
    let parser = Parser::new(default_time, arena);
    parser.parse(lines)
}
