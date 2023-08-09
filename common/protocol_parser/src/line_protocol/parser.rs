use bumpalo::Bump;

use crate::{
    check_pos_valid, next_field_set, next_measurement, next_tag_set, next_value, Error, Line,
    Result,
};

pub struct Parser<'a> {
    default_time: i64,
    arena: &'a Bump,
}

impl<'a> Parser<'a> {
    pub fn new(default_time: i64, arena: &'a Bump) -> Parser {
        Self {
            default_time,
            arena,
        }
    }

    pub fn parse(&self, lines: &'a str) -> Result<Vec<Line<'a>, &'a Bump>> {
        let mut ret = Vec::new_in(self.arena);
        let mut pos = 0_usize;
        while let Some((mut line, offset)) = self.next_line(lines, pos)? {
            line.sort_and_dedup();
            ret.push(line);
            pos += offset;
        }
        Ok(ret)
    }

    fn next_line(&self, buf: &'a str, position: usize) -> Result<Option<(Line<'a>, usize)>> {
        if position > buf.len() {
            return Ok(None);
        }

        let start_pos = position;
        let mut pos = position;
        let measurement = if let Some(m) = next_measurement(&buf[pos..]) {
            pos += m.1;
            m.0
        } else {
            return Ok(None);
        };
        check_pos_valid(buf, pos)?;

        let tags = if let Some(t) = next_tag_set(&buf[pos..]) {
            pos += t.1;
            t.0
        } else {
            return Err(Error::Parse {
                pos,
                content: String::from(buf),
            });
        };
        check_pos_valid(buf, pos)?;

        let fields = if let Some(f) = next_field_set(&buf[pos..])? {
            pos += f.1;
            f.0
        } else {
            return Err(Error::Parse {
                pos,
                content: String::from(buf),
            });
        };

        let timestamp = if pos < buf.len() {
            if let Some(t) = next_value(&buf[pos..]) {
                let timestamp = t.0.parse::<i64>().map_err(|e| Error::Parse {
                    pos,
                    content: format!("{}: '{}'", e, buf),
                })?;
                pos += t.1;
                timestamp
            } else {
                self.default_time
            }
        } else {
            self.default_time
        };

        Ok(Some((
            Line {
                hash_id: 0,
                table: measurement,
                tags,
                fields,
                timestamp,
            },
            pos - start_pos,
        )))
    }
}

#[cfg(test)]
mod test {
    use std::fs::File;
    use std::io::Read;

    use minivec::MiniVec;
    use protos::FieldValue;

    use crate::line_protocol::parser::{Line, Parser};
    use crate::{next_field_set, next_measurement, next_tag_set, next_value};

    #[test]
    fn test_parse_functions() {
        //! measurement: ma
        //! | ta  | tb | fa       | fb | ts |
        //! | --  | -- | ------   | -- | -- |
        //! | 2\\ | 1  | "112\"3" | 2  | 1  |
        //!
        //! measurement: mb
        //! | tb | tc  | fa  | fc  | ts |
        //! | -- | --- | --- | --- | -- |
        //! | 2  | abc | 1.3 | 0.9 |    |

        let lines = "ma,ta=2\\\\,tb=1 fa=\"112\\\"3\",fb=2 1  \n mb,tb=2,tc=abc fa=1.3,fc=0.9";
        println!(
            "Length of the line protocol string in test case: {}\n======\n{}\n======",
            lines.len(),
            lines
        );

        let mut pos = 0;
        let measurement = next_measurement(&lines[pos..]).unwrap();
        assert_eq!(measurement, ("ma", 3));
        pos += measurement.1;

        if pos < lines.len() {
            let tagset = next_tag_set(&lines[pos..]).unwrap();
            assert_eq!(tagset, (vec![("ta", "2\\\\"), ("tb", "1")], 12));
            pos += tagset.1;
        }

        if pos < lines.len() {
            let fieldset = next_field_set(&lines[pos..]).unwrap().unwrap();
            assert_eq!(
                fieldset,
                (
                    vec![
                        ("fa", FieldValue::Str(MiniVec::from(b"112\\\"3".as_slice()))),
                        ("fb", FieldValue::F64(2.0)),
                    ],
                    17
                )
            );
            pos += fieldset.1;
        }

        if pos < lines.len() {
            let timestamp = next_value(&lines[pos..]);
            assert_eq!(timestamp, Some(("1", 2)));
            pos += timestamp.unwrap().1;
        }

        println!("==========");

        if pos < lines.len() {
            let measurement = next_measurement(&lines[pos..]).unwrap();
            assert_eq!(measurement, ("mb", 6));
            pos += measurement.1;
        }

        if pos < lines.len() {
            let tagset = next_tag_set(&lines[pos..]).unwrap();
            assert_eq!(tagset, (vec![("tb", "2"), ("tc", "abc")], 12));
            pos += tagset.1;
        }

        if pos < lines.len() {
            let fieldset = next_field_set(&lines[pos..]).unwrap().unwrap();
            assert_eq!(
                fieldset,
                (
                    vec![("fa", FieldValue::F64(1.3)), ("fc", FieldValue::F64(0.9))],
                    14
                )
            );
            pos += fieldset.1;
        }

        if pos < lines.len() {
            let timestamp = next_value(&lines[pos..]);
            assert_eq!(timestamp, None);
        }
    }

    #[test]
    fn test_line_parser() {
        //! measurement: ma
        //! | ta  | tb | fa     | fb | fc             | ts |
        //! | --  | -- | ------ | -- | -------------- | -- |
        //! | 2\\ | 1  | 112\"" | 2  | "hello, world" | 1  |
        //!
        //! measurement: mb
        //! | tb | tc  | fa  | fc  | ts |
        //! | -- | --- | --- | --- | -- |
        //! | 2  | abc | 1.3 | 0.9 |    |

        let lines = "ma,ta=2\\\\,tb=1 fa=\"112\\\"3\",fb=2,fc=\"hello, world\" 1  \n mb,tb=2,tc=abc fa=1.3,fc=0.9";
        println!(
            "Length of the line protocol string in test case: {}\n======\n{}\n======",
            lines.len(),
            lines
        );

        let bump = bumpalo::Bump::new();
        let parser = Parser::new(-1, &bump);
        let data = parser.parse(lines).unwrap();
        assert_eq!(data.len(), 2);

        let data_1 = data.get(0).unwrap();
        assert_eq!(
            *data_1,
            Line {
                hash_id: 0,
                table: "ma",
                tags: vec![("ta", "2\\\\"), ("tb", "1")],
                fields: vec![
                    ("fa", FieldValue::Str(MiniVec::from(b"112\\\"3".as_slice()))),
                    ("fb", FieldValue::F64(2.0)),
                    (
                        "fc",
                        FieldValue::Str(MiniVec::from(b"hello, world".as_slice()))
                    ),
                ],
                timestamp: 1,
            }
        );

        let data_2 = data.get(1).unwrap();
        assert_eq!(
            *data_2,
            Line {
                hash_id: 0,
                table: "mb",
                tags: vec![("tb", "2"), ("tc", "abc")],
                fields: vec![("fa", FieldValue::F64(1.3)), ("fc", FieldValue::F64(0.9))],
                timestamp: -1,
            }
        );
    }

    #[test]
    #[ignore]
    fn test_generated_data() {
        let mut lp_file = File::open("/tmp/cnosdb-data").unwrap();
        let mut lp_lines = String::new();
        lp_file.read_to_string(&mut lp_lines).unwrap();

        let bump = bumpalo::Bump::new();
        let parser = Parser::new(-1, &bump);
        let lines = parser.parse(&lp_lines).unwrap();

        for l in lines {
            println!("{:?}", l);
        }
    }

    #[test]
    fn test_unicode() {
        let bump = bumpalo::Bump::new();
        let parser = Parser::new(-1, &bump);
        let lp = parser.parse("m,t1=中,t2=发,t3=majh f=\"白\"").unwrap();
        assert_eq!(lp.len(), 1);
        assert_eq!(lp[0].table, "m");
        assert_eq!(lp[0].tags.len(), 3);
        assert_eq!(lp[0].tags[0], ("t1", "中"));
        assert_eq!(lp[0].tags[1], ("t2", "发"));
        assert_eq!(lp[0].tags[2], ("t3", "majh"));
        assert_eq!(lp[0].fields.len(), 1);
        assert_eq!(
            lp[0].fields[0],
            ("f", FieldValue::Str(MiniVec::from("白".as_bytes())))
        );
    }

    #[test]
    fn test_alloc() {
        use bumpalo::Bump;

        // Create a new bump arena.
        let bump = Bump::new();

        // Create a `Vec` whose elements are allocated within the bump arena.
        let mut v = Vec::new_in(&bump);
        v.push(0);
        v.push(1);
        v.push(2);
    }
}
