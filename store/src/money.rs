use bigdecimal::num_bigint::Sign;
use bigdecimal::BigDecimal;
use std::str::FromStr;

/// Errors converting a human decimal-string amount into exact integer base units.
#[derive(thiserror::Error, Debug, PartialEq, Eq)]
pub enum MoneyError {
    #[error("amount is not a valid decimal number")]
    Invalid,
    #[error("amount must not be negative")]
    Negative,
    #[error("amount has more fractional digits than the asset supports")]
    TooManyDecimals,
}

/// Convert a decimal **string** (e.g. `"1.5"`) into exact base units for an asset with
/// `decimals` fractional digits (e.g. USDC = 6 → `"1.5"` becomes `1500000`).
///
/// The whole pipeline stays on `BigDecimal`; there is no `f64` anywhere, so there is no
/// binary-float drift. An amount with more fractional precision than the asset supports is
/// rejected rather than silently rounded.
pub fn parse_base_units(amount: &str, decimals: u32) -> Result<BigDecimal, MoneyError> {
    let parsed = BigDecimal::from_str(amount.trim()).map_err(|_| MoneyError::Invalid)?;

    if parsed.sign() == Sign::Minus {
        return Err(MoneyError::Negative);
    }

    let scaled = parsed * BigDecimal::from(10u64.pow(decimals));
    if !scaled.is_integer() {
        return Err(MoneyError::TooManyDecimals);
    }

    // Already integer-valued, so fixing the scale to 0 is exact (no rounding).
    Ok(scaled.with_scale(0))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bd(s: &str) -> BigDecimal {
        BigDecimal::from_str(s).unwrap()
    }

    #[test]
    fn converts_decimal_strings_to_base_units_exactly() {
        // (input, decimals, expected base units) — exact, no float drift.
        let cases = [
            ("1", 6, "1000000"),
            ("1.5", 6, "1500000"),
            ("0.000001", 6, "1"),
            ("123.456789", 6, "123456789"),
            ("0", 6, "0"),
            ("1000000", 6, "1000000000000"),
            // amounts that famously break binary floats:
            ("0.1", 6, "100000"),
            ("0.3", 6, "300000"),
            ("0.07", 6, "70000"),
            ("4.35", 2, "435"),
            // trailing zeros beyond the asset precision are fine (value is exact):
            ("1.5000000", 6, "1500000"),
            // SOL-scale (9 decimals):
            ("2.5", 9, "2500000000"),
        ];
        for (input, decimals, expected) in cases {
            let got = parse_base_units(input, decimals)
                .unwrap_or_else(|e| panic!("{input} @ {decimals}dp should convert, got {e:?}"));
            assert_eq!(got, bd(expected), "{input} @ {decimals}dp");
            // base units are whole integers
            assert!(got.is_integer());
        }
    }

    #[test]
    fn rejects_excess_precision_negatives_and_garbage() {
        assert_eq!(
            parse_base_units("0.0000001", 6),
            Err(MoneyError::TooManyDecimals)
        );
        assert_eq!(parse_base_units("1.005", 2), Err(MoneyError::TooManyDecimals));
        assert_eq!(parse_base_units("-1", 6), Err(MoneyError::Negative));
        assert_eq!(parse_base_units("-0.5", 6), Err(MoneyError::Negative));
        for garbage in ["", "abc", "1.2.3", "1,5", "NaN", "0x10", "  ", "1e", "$5"] {
            assert_eq!(
                parse_base_units(garbage, 6),
                Err(MoneyError::Invalid),
                "garbage {garbage:?} must be rejected"
            );
        }
    }
}
