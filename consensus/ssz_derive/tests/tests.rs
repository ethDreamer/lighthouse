use ssz::{Decode, Encode};
use ssz_derive::{Decode, Encode};
use std::fmt::Debug;

fn assert_encode<T: Encode>(item: &T, bytes: &[u8]) {
    assert_eq!(item.as_ssz_bytes(), bytes);
}

fn assert_encode_decode<T: Encode + Decode + PartialEq + Debug>(item: &T, bytes: &[u8]) {
    assert_encode(item, bytes);
    assert_eq!(T::from_ssz_bytes(bytes).unwrap(), *item);
}

#[derive(PartialEq, Debug, Encode, Decode)]
#[ssz(enum_behaviour = "union")]
enum TwoFixedUnion {
    U8(u8),
    U16(u16),
}

#[derive(PartialEq, Debug, Encode, Decode)]
struct TwoFixedUnionStruct {
    a: TwoFixedUnion,
}

#[test]
fn two_fixed_union() {
    let eight = TwoFixedUnion::U8(1);
    let sixteen = TwoFixedUnion::U16(1);

    assert_encode_decode(&eight, &[0, 1]);
    assert_encode_decode(&sixteen, &[1, 1, 0]);

    assert_encode_decode(&TwoFixedUnionStruct { a: eight }, &[4, 0, 0, 0, 0, 1]);
    assert_encode_decode(&TwoFixedUnionStruct { a: sixteen }, &[4, 0, 0, 0, 1, 1, 0]);
}

#[derive(PartialEq, Debug, Encode, Decode)]
struct VariableA {
    a: u8,
    b: Vec<u8>,
}

#[derive(PartialEq, Debug, Encode, Decode)]
struct VariableB {
    a: Vec<u8>,
    b: u8,
}

#[derive(PartialEq, Debug, Encode)]
#[ssz(enum_behaviour = "transparent")]
enum TwoVariableTrans {
    A(VariableA),
    B(VariableB),
}

#[derive(PartialEq, Debug, Encode)]
struct TwoVariableTransStruct {
    a: TwoVariableTrans,
}

#[derive(PartialEq, Debug, Encode, Decode)]
#[ssz(enum_behaviour = "union")]
enum TwoVariableUnion {
    A(VariableA),
    B(VariableB),
}

#[derive(PartialEq, Debug, Encode, Decode)]
struct TwoVariableUnionStruct {
    a: TwoVariableUnion,
}

#[test]
fn two_variable_trans() {
    let trans_a = TwoVariableTrans::A(VariableA {
        a: 1,
        b: vec![2, 3],
    });
    let trans_b = TwoVariableTrans::B(VariableB {
        a: vec![1, 2],
        b: 3,
    });

    assert_encode(&trans_a, &[1, 5, 0, 0, 0, 2, 3]);
    assert_encode(&trans_b, &[5, 0, 0, 0, 3, 1, 2]);

    assert_encode(
        &TwoVariableTransStruct { a: trans_a },
        &[4, 0, 0, 0, 1, 5, 0, 0, 0, 2, 3],
    );
    assert_encode(
        &TwoVariableTransStruct { a: trans_b },
        &[4, 0, 0, 0, 5, 0, 0, 0, 3, 1, 2],
    );
}

#[test]
fn two_variable_union() {
    let union_a = TwoVariableUnion::A(VariableA {
        a: 1,
        b: vec![2, 3],
    });
    let union_b = TwoVariableUnion::B(VariableB {
        a: vec![1, 2],
        b: 3,
    });

    assert_encode_decode(&union_a, &[0, 1, 5, 0, 0, 0, 2, 3]);
    assert_encode_decode(&union_b, &[1, 5, 0, 0, 0, 3, 1, 2]);

    assert_encode_decode(
        &TwoVariableUnionStruct { a: union_a },
        &[4, 0, 0, 0, 0, 1, 5, 0, 0, 0, 2, 3],
    );
    assert_encode_decode(
        &TwoVariableUnionStruct { a: union_b },
        &[4, 0, 0, 0, 1, 5, 0, 0, 0, 3, 1, 2],
    );
}
