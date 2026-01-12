use nom::{
    branch::alt,
    bytes::complete::{tag_no_case, take_until, take_while},
    character::complete::{char, digit1, multispace0, multispace1},
    combinator::{map_res, opt, recognize},
    multi::separated_list1,
    sequence::{delimited, preceded, tuple, terminated},
    IResult,
};
use uuid::Uuid;

#[derive(Debug, PartialEq, Clone)]
pub enum Command {
    Insert { vector: Vec<f32>, payload: String, id: Option<Uuid> },
    Select { vector: Option<Vec<f32>>, filter_id: Option<Uuid>, as_of: Option<u64>, limit: usize },
    Update { id: Uuid, vector: Option<Vec<f32>>, payload: Option<String> },
    Delete { id: Uuid },
    Get { id: Uuid },
    History { id: Uuid },
    Help,
    Exit,
}

// --- BASIC PARSERS ---

fn parse_float(input: &str) -> IResult<&str, f32> {
    let (input, num_str) = recognize(tuple((
        opt(char('-')),
                                            digit1,
                                            opt(tuple((char('.'), digit1))),
    )))(input)?;
    match num_str.parse::<f32>() {
        Ok(n) => Ok((input, n)),
        Err(_) => Err(nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Float))),
    }
}

fn parse_u64(input: &str) -> IResult<&str, u64> {
    map_res(digit1, |s: &str| s.parse::<u64>())(input)
}

fn parse_vector(input: &str) -> IResult<&str, Vec<f32>> {
    delimited(
        char('['),
              separated_list1(tuple((multispace0, char(','), multispace0)), parse_float),
              char(']')
    )(input)
}

fn parse_quoted_string(input: &str) -> IResult<&str, String> {
    let (input, _) = char('"')(input)?;
    let (input, content) = take_until("\"")(input)?;
    let (input, _) = char('"')(input)?;
    Ok((input, content.to_string()))
}

fn parse_uuid(input: &str) -> IResult<&str, Uuid> {
    let (input, uuid_str) = take_while(|c: char| c.is_digit(16) || c == '-')(input)?;
    match Uuid::parse_str(uuid_str) {
        Ok(uuid) => Ok((input, uuid)),
        Err(_) => Err(nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Tag))),
    }
}

// --- HELPERS ---
fn ws<'a, F, O, E: nom::error::ParseError<&'a str>>(inner: F) -> impl FnMut(&'a str) -> IResult<&'a str, O, E>
where F: FnMut(&'a str) -> IResult<&'a str, O, E> {
    delimited(multispace0, inner, multispace0)
}

fn tag_ci(t: &'static str) -> impl FnMut(&str) -> IResult<&str, &str> {
    move |input| tag_no_case(t)(input)
}

// --- COMMAND PARSERS ---

fn parse_insert(input: &str) -> IResult<&str, Command> {
    let (input, _) = tag_ci("INSERT")(input)?;
    let (input, _) = opt(tuple((ws(tag_ci("INTO")), ws(tag_ci("VECTORS")))))(input)?;
    let (input, _) = opt(ws(tag_ci("VALUES")))(input)?;

    let (input, _) = ws(char('('))(input)?;
    let (input, vector) = parse_vector(input)?;
    let (input, _) = ws(char(','))(input)?;
    let (input, payload) = parse_quoted_string(input)?;

    let (input, id) = opt(preceded(
        ws(char(',')),
                                   delimited(opt(char('\'')), parse_uuid, opt(char('\'')))
    ))(input)?;

    let (input, _) = ws(char(')'))(input)?;
    Ok((input, Command::Insert { vector, payload, id }))
}

fn parse_select(input: &str) -> IResult<&str, Command> {
    let (input, _) = alt((tag_ci("SELECT"), tag_ci("FIND")))(input)?;
    let (input, _) = multispace1(input)?;
    let (input, _) = opt(tuple((take_until("FROM"), tag_ci("FROM"), multispace1, tag_ci("VECTORS"))))(input)?;
    let (input, _) = opt(ws(tag_ci("WHERE")))(input)?;

    let (input, vector) = opt(preceded(
        tuple((ws(tag_ci("VECTOR")), ws(tag_ci("NEAR")))),
                                       parse_vector
    ))(input)?;

    let (input, _) = opt(ws(tag_ci("AND")))(input)?;

    let (input, filter_id) = opt(preceded(
        tuple((ws(tag_ci("ID")), ws(char('=')), opt(char('\'')))),
                                          terminated(parse_uuid, opt(char('\'')))
    ))(input)?;

    let (input, as_of) = opt(preceded(
        tuple((ws(tag_ci("AS")), ws(tag_ci("OF")))),
                                      parse_u64
    ))(input)?;

    let (input, limit) = opt(preceded(
        ws(tag_ci("LIMIT")),
                                      map_res(digit1, |s: &str| s.parse::<usize>())
    ))(input)?;

    Ok((input, Command::Select { vector, filter_id, as_of, limit: limit.unwrap_or(5) }))
}

fn parse_update(input: &str) -> IResult<&str, Command> {
    let (input, _) = tag_ci("UPDATE")(input)?;
    let (input, _) = opt(ws(tag_ci("VECTORS")))(input)?;
    let (input, _) = ws(tag_ci("SET"))(input)?;
    let (input, _) = ws(tag_ci("PAYLOAD"))(input)?;
    let (input, _) = ws(char('='))(input)?;
    let (input, payload) = parse_quoted_string(input)?;
    let (input, _) = ws(tag_ci("WHERE"))(input)?;
    let (input, _) = ws(tag_ci("ID"))(input)?;
    let (input, _) = ws(char('='))(input)?;
    let (input, _) = opt(char('\''))(input)?;
    let (input, id) = parse_uuid(input)?;
    let (input, _) = opt(char('\''))(input)?;
    Ok((input, Command::Update { id, payload: Some(payload), vector: None }))
}

fn parse_delete(input: &str) -> IResult<&str, Command> {
    let (input, _) = tag_ci("DELETE")(input)?;
    let (input, _) = opt(tuple((ws(tag_ci("FROM")), ws(tag_ci("VECTORS")))))(input)?;
    let (input, _) = ws(tag_ci("WHERE"))(input)?;
    let (input, _) = ws(tag_ci("ID"))(input)?;
    let (input, _) = ws(char('='))(input)?;
    let (input, _) = opt(char('\''))(input)?;
    let (input, id) = parse_uuid(input)?;
    let (input, _) = opt(char('\''))(input)?;
    Ok((input, Command::Delete { id }))
}

fn parse_get(input: &str) -> IResult<&str, Command> {
    let (input, _) = tag_ci("GET")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, _) = opt(char('\''))(input)?;
    let (input, id) = parse_uuid(input)?;
    let (input, _) = opt(char('\''))(input)?;
    Ok((input, Command::Get { id }))
}

fn parse_history(input: &str) -> IResult<&str, Command> {
    let (input, _) = tag_ci("HISTORY")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, _) = opt(char('\''))(input)?;
    let (input, id) = parse_uuid(input)?;
    let (input, _) = opt(char('\''))(input)?;
    Ok((input, Command::History { id }))
}

fn parse_help(input: &str) -> IResult<&str, Command> {
    let (input, _) = tag_ci("HELP")(input)?;
    Ok((input, Command::Help))
}

fn parse_exit(input: &str) -> IResult<&str, Command> {
    let (input, _) = alt((tag_ci("EXIT"), tag_ci("QUIT")))(input)?;
    Ok((input, Command::Exit))
}

pub fn parse_command(input: &str) -> Result<Command, String> {
    let input = input.trim();
    let result = alt((
        parse_insert,
        parse_select,
        parse_update,
        parse_delete,
        parse_get,
        parse_history,
        parse_help,
        parse_exit,
    ))(input);

    match result {
        Ok((remainder, cmd)) => {
            if !remainder.trim().is_empty() {
                return Err(format!("Unexpected tokens at end: '{}'", remainder));
            }
            Ok(cmd)
        },
        Err(nom::Err::Error(e)) | Err(nom::Err::Failure(e)) => {
            // e.input contains the slice where parsing failed
            let context = if e.input.len() > 20 {
                format!("{}...", &e.input[..20])
            } else {
                e.input.to_string()
            };
            Err(format!("Invalid syntax near: '{}'", context))
        },
        Err(nom::Err::Incomplete(_)) => Err("Incomplete command.".to_string()),
    }
}
