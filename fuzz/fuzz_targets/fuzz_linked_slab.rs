#![no_main]
use libfuzzer_sys::fuzz_target;
use quick_cache::linked_slab::LinkedSlab;
use std::collections::BTreeMap;

fuzz_target!(|ops: Vec<Result<u16, u16>>| {
    // The model is a BTreeMap from key to token.
    // The key sort order is the order that we want to keep items in the slab circular lists.
    // Run three models in parallel, which means keeping 3 independent circular lists within the slab.
    let mut slab = LinkedSlab::<u16>::with_capacity(0);
    let mut models = (0..3).map(|_| BTreeMap::new()).collect::<Vec<_>>();
    for op in ops {
        for model in &mut models {
            match op {
                Ok(i) => {
                    match model.range(&i..).next() {
                        Some((gte, _)) if gte == &i => {
                            // already exists
                            continue;
                        }
                        Some((_, &next_token)) => {
                            // insert i before next_token, which is the token for a key > i
                            let new_token = slab.insert(i, Some(next_token));
                            model.insert(i, new_token);
                        }
                        None => {
                            // insert i before the very first token,
                            // which is the same as inserting at the end
                            let first_token = model.values().next().copied();
                            let new_token = slab.insert(i, first_token);
                            model.insert(i, new_token);
                        }
                    }
                }
                Err(i) => {
                    if let Some(token) = model.remove(&i) {
                        let (j, next_token) = slab.remove(token).unwrap();
                        assert_eq!(j, i);
                        // the next token is either
                        // * the token for the next key
                        // * the token for the first key (circular list)
                        // * None
                        let expected_next_token = model
                            .range(i..)
                            .next()
                            .or(model.iter().next())
                            .map(|(_, t)| *t);
                        assert_eq!(next_token, expected_next_token);
                    }
                }
            }
        }
    }
    for model in &models {
        if model.is_empty() {
            continue;
        }
        let first_token = *model.values().next().unwrap();
        let mut token = first_token;
        for (i, &expected_token) in model {
            assert_eq!(token, expected_token);
            let (j, next) = slab.get(token).unwrap();
            assert_eq!(j, i);
            token = next;
        }
        // it's a circular list
        assert_eq!(token, first_token);
    }
    assert_eq!(slab.len(), models.iter().map(|m| m.len()).sum());
});
