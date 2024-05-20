#![no_main]
use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use quick_cache::linked_slab::LinkedSlab;
use std::collections::BTreeMap;

#[derive(Debug, Arbitrary)]
enum Op {
    Add(u16),
    Remove(u16),
    Unlink(u16),
    Link(u16),
}

fuzz_target!(|ops: Vec<Op>| {
    // The model is a BTreeMap from key to token.
    // The key sort order is the order that we want to keep items in the slab circular lists.
    // Run three models in parallel, which means keeping 3 independent circular lists within the slab.
    let mut slab = LinkedSlab::<u16>::with_capacity(0);
    let mut models = (0..3).map(|_| BTreeMap::new()).collect::<Vec<_>>();
    let mut unlinked = BTreeMap::new();
    for op in ops {
        for model in &mut models {
            match op {
                Op::Add(i) => {
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
                    break;
                }
                Op::Remove(i) => {
                    let Some(token) = model.remove(&i) else {
                        continue;
                    };
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
                    break;
                }
                Op::Link(i) => {
                    let Some(&token) = unlinked.get(&i) else {
                        break;
                    };
                    match model.range(&i..).next() {
                        Some((gte, _)) if gte == &i => {
                            // already exists
                            continue;
                        }
                        Some((_, &next_token)) => {
                            // insert i before next_token, which is the token for a key > i
                            slab.link(token, Some(next_token));
                        }
                        None => {
                            // insert i before the very first token,
                            // which is the same as inserting at the end
                            let first_token = model.values().next().copied();
                            slab.link(token, first_token);
                        }
                    }
                    model.insert(i, token);
                    unlinked.remove(&i);
                    break;
                }
                Op::Unlink(i) => {
                    if unlinked.contains_key(&i) {
                        break;
                    }
                    let Some(token) = model.remove(&i) else {
                        continue;
                    };
                    let next_token = slab.unlink(token);
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
                    unlinked.insert(i, token);
                    break;
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
    for (&i, &token) in &unlinked {
        let (g, next) = slab.get(token).unwrap();
        assert_eq!(i, *g);
        assert_eq!(next, token);
    }
    assert_eq!(
        slab.len(),
        unlinked.len() + models.iter().map(|m| m.len()).sum::<usize>()
    );
});
