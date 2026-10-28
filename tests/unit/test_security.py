from foundry.security import (
    constant_time_equals,
    generate_api_key,
    hash_secret,
    parse_api_key,
    verify_secret,
)

PEPPER = "pepper"


def test_generated_key_roundtrip():
    key = generate_api_key(PEPPER)
    parsed = parse_api_key(key.token)
    assert parsed is not None
    prefix, secret = parsed
    assert prefix == key.prefix
    assert verify_secret(secret, PEPPER, key.secret_hash)


def test_secret_is_not_stored_in_clear_and_pepper_matters():
    key = generate_api_key(PEPPER)
    _, secret = parse_api_key(key.token)
    assert secret not in key.secret_hash
    assert not verify_secret(secret, "other-pepper", key.secret_hash)
    assert hash_secret(secret, PEPPER) == key.secret_hash


def test_keys_are_unique():
    assert len({generate_api_key(PEPPER).token for _ in range(200)}) == 200


def test_parse_rejects_garbage():
    for bad in ["", "nope", "fnd_", "fnd_short_secret", "xxx_abcdef12_secret", "fnd_abcdef12_"]:
        assert parse_api_key(bad) is None


def test_secret_with_underscores_survives_parsing():
    parsed = parse_api_key("fnd_abcdef12_se_cr-et_x")
    assert parsed == ("abcdef12", "se_cr-et_x")


def test_constant_time_equals():
    assert constant_time_equals("abc", "abc")
    assert not constant_time_equals("abc", "abd")
