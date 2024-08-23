use crate::rsmp::Rsmp;

use bytes::Bytes;
use nom::branch::alt;
use nom::bytes::complete::{tag_no_case, take_until, take_while};
use nom::combinator::{not, opt};
use nom::number::complete::double;
use nom::{Err, IResult};
use thiserror::Error;

#[derive(Clone, Error, Debug)]
pub enum ParseError {
    #[error("Failed to parse remaining bytes: {0}")]
    IncompleteParse(String),
    #[error("Unexpected input: {0}")]
    UnexpectedInput(String),
    #[error("Unknown Error: {0}")]
    UnknownError(String),
}

pub fn parse(input: &[u8]) -> Result<Rsmp, ParseError> {
    match alt((array, element))(input) {
        Ok((b, rsmp)) if is_null_slice(b) => Ok(rsmp),
        Ok((b, _m)) => {
            Err(ParseError::IncompleteParse(
                String::from_utf8_lossy(b).into(),
            ))
        }
        Err(Err::Error(e)) => Err(ParseError::UnexpectedInput(
            String::from_utf8_lossy(rm_trailing_nulls(e.input)).into(),
        )),
        Err(e) => Err(ParseError::UnknownError(e.to_string())),
    }
}

fn element(input: &[u8]) -> IResult<&[u8], Rsmp> {
    alt((
        simple_string,
        simple_error,
        float,
        int,
        array,
        raw_bytes,
        null,
    ))(input)
}

pub fn null(input: &[u8]) -> IResult<&[u8], Rsmp> {
    let (input, _) = tag_no_case("~")(input)?;
    let (input, _) = crlf(input)?;
    Ok((input, Rsmp::Null))
}

pub fn raw_bytes(input: &[u8]) -> IResult<&[u8], Rsmp> {
    let (input, _) = tag_no_case("@")(input)?;
    let (input, b) = take_until("\r\n")(input)?;
    let (input, _) = crlf(input)?;
    Ok((input, Rsmp::RawBytes(Bytes::copy_from_slice(b))))
}

pub fn array(input: &[u8]) -> IResult<&[u8], Rsmp> {
    let (input, _) = tag_no_case("+")(input)?;
    let (input, n) = i64_from_digits(input)?;
    let (input, _) = crlf(input)?;
    let (input, array) = array_elements(input, n)?;
    Ok((input, Rsmp::Array(array)))
}

fn array_elements(input: &[u8], n: i64) -> IResult<&[u8], Vec<Rsmp>> {
    let mut elements = Vec::new();
    let mut input = input;
    for _ in 0..n {
        let (i, element) = element(input)?;
        elements.push(element);
        input = i;
    }
    Ok((input, elements))
}

fn simple_string(input: &[u8]) -> IResult<&[u8], Rsmp> {
    let (input, _) = tag_no_case("$")(input)?;
    let (input, _) = opt(crlf)(input)?;
    let (input, s) = take_until("\r\n")(input)?;
    let (input, _) = crlf(input)?;
    Ok((input, Rsmp::String(Bytes::copy_from_slice(s))))
}

fn simple_error(input: &[u8]) -> IResult<&[u8], Rsmp> {
    let (input, _) = tag_no_case("!")(input)?;
    let (input, _) = opt(crlf)(input)?;
    let (input, s) = take_until("\r\n")(input)?;
    let (input, _) = crlf(input)?;
    Ok((input, Rsmp::Error(Bytes::copy_from_slice(s))))
}

fn int(input: &[u8]) -> IResult<&[u8], Rsmp> {
    let (input, _) = tag_no_case(":")(input)?;
    let (input, i) = i64_from_digits(input)?;
    let (input, _) = crlf(input)?;
    Ok((input, Rsmp::Int(i)))
}

fn float(input: &[u8]) -> IResult<&[u8], Rsmp> {
    let (input, _) = tag_no_case("^")(input)?;
    let (input, i) = f64_from_digits(input)?;
    let (input, _) = crlf(input)?;
    Ok((input, Rsmp::Float(i)))
}

fn f64_from_digits(input: &[u8]) -> IResult<&[u8], f64> {
    double(input)
}

fn i64_from_digits(input: &[u8]) -> IResult<&[u8], i64> {
    let (input, i) = take_while(|b: u8| b.is_ascii_digit())(input)?;
    let (input, _) = not(tag_no_case("."))(input)?;
    let i = match std::str::from_utf8(i) {
        #[allow(clippy::from_str_radix_10)]
        Ok(s) => match i64::from_str_radix(s, 10) {
            Ok(num) => num,
            Err(_) => {
                return Err(nom::Err::Failure(nom::error::Error::new(
                    input,
                    nom::error::ErrorKind::Digit,
                )))
            }
        },
        Err(_) => {
            return Err(nom::Err::Failure(nom::error::Error::new(
                input,
                nom::error::ErrorKind::Digit,
            )))
        }
    };
    Ok((input, i))
}

fn crlf(input: &[u8]) -> IResult<&[u8], ()> {
    let (input, _) = tag_no_case("\r\n")(input)?;
    Ok((input, ()))
}

fn is_null_slice(b: &[u8]) -> bool {
    b.iter().all(|b| *b == 0)
}

fn rm_trailing_nulls(b: &[u8]) -> &[u8] {
    let mut len = b.len();
    while len > 0 && b[len - 1] == b'\0' {
        len -= 1;
    }

    &b[..len]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_null() {
        let result = parse(b"~\r\n").unwrap();
        assert_eq!(result, Rsmp::Null);
    }

    #[test]
    fn parse_i64() {
        let result = parse(b":1\r\n").unwrap();
        assert_eq!(result, Rsmp::Int(1));
        let result = parse(b":69420\r\n").unwrap();
        assert_eq!(result, Rsmp::Int(69420));
    }

    #[test]
    fn parse_string() {
        let result = parse(b"$hello\r\n").unwrap();
        assert_eq!(result, Rsmp::String(b"hello"[..].into()));

        let result = parse(b"$\r\nhello\r\n").unwrap();
        assert_eq!(result, Rsmp::String(b"hello"[..].into()));
    }

    #[test]
    fn parse_float() {
        let result = parse(b"^1.0\r\n").unwrap();
        assert_eq!(result, Rsmp::Float(1.0));

        let result = parse(b"^69.420\r\n").unwrap();
        assert_eq!(result, Rsmp::Float(69.420));
    }

    #[test]
    fn parse_error() {
        let result = parse(b"!error\r\n").unwrap();
        assert_eq!(result, Rsmp::Error(b"error"[..].into()));
    }

    #[test]
    fn parse_array() {
        let result = parse(b"+2\r\n$hello\r\n:1\r\n").unwrap();
        assert_eq!(
            result,
            Rsmp::Array(vec![Rsmp::String(b"hello"[..].into()), Rsmp::Int(1)])
        );
    }
}
