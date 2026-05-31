use cdDB::{WriteCommand, Attributes};

#[test]
fn test_encode_decode_roundtrip() {
    let mut attrs_str = Attributes::new();
    attrs_str.insert("name".to_string(), "test_name".to_string());
    
    let mut attrs_int = Attributes::new();
    attrs_int.insert("val".to_string(), 42);
    
    let mut attrs_blob = Attributes::new();
    attrs_blob.insert("blob_data".to_string(), vec![1, 2, 3]);

    let cmd = WriteCommand::Insert {
        entity_id: 123,
        attributes: attrs_str,
        attributes_int: attrs_int,
        attributes_blob: attrs_blob,
    };
    
    let encoded = cmd.encode();
    let decoded = WriteCommand::decode(&encoded).expect("Decode should succeed");
    
    match decoded {
        WriteCommand::Insert { entity_id, attributes, attributes_int, attributes_blob } => {
            assert_eq!(entity_id, 123);
            assert_eq!(attributes_int.inner().get("val"), Some(&42));
            assert_eq!(attributes_blob.inner().get("blob_data"), Some(&vec![1, 2, 3]));
            assert_eq!(attributes.inner().get("name"), Some(&"test_name".to_string()));
        },
        _ => panic!("Decoded command is not an Insert"),
    }
}
