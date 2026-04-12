pub struct Error;

#[inline(always)]
fn digit(c: u8) -> Result<u8, Error> {
    c.is_ascii_digit().then_some(c - b'0').ok_or(Error)
}

#[inline(always)]
pub fn digit2(x: [u8; 2]) -> Result<u8, Error> {
    let x0 = digit(x[0])?;
    let x1 = digit(x[1])?;
    Ok(x0 * 10 + x1)
}

#[inline(always)]
pub fn digit4(x: [u8; 4]) -> Result<u16, Error> {
    let x0 = u16::from(digit2([x[0], x[1]])?);
    let x1 = u16::from(digit2([x[2], x[3]])?);
    Ok(x0 * 100 + x1)
}

pub fn consume<I, O, F>(input: &mut I, f: F) -> Result<O, nom::Err<nom::error::Error<I>>>
where
    F: FnOnce(I) -> nom::IResult<I, O>,
    I: Copy,
{
    let (remaining, output) = f(*input)?;
    *input = remaining;
    Ok(output)
}
