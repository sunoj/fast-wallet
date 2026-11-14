// Node answers proving a pool already holds the exact signed bytes of a send.
// Exports exact-match predicates for raw JSON-RPC messages and RpcClient's error text.
use alloy::primitives::B256;

/// True when an `eth_sendRawTransaction` error message says the node already holds the
/// transaction hashing to `tx_hash`. Pools key that answer by transaction hash, which
/// commits to the signed bytes (a different transaction at the same nonce is "replacement
/// transaction underpriced"), so the send is in that node's pool. The whole message must
/// match: "already known" (geth, op-geth, reth), "Known transaction" (Besu), or
/// "known transaction: <hash>" (legacy geth) whose hash is `tx_hash`. A message that only
/// contains the phrase, such as a revert reason or a compound proxy error, does not match.
pub(crate) fn is_already_known(message: &str, tx_hash: &B256) -> bool {
    let message = message.trim().to_ascii_lowercase();
    if message == "already known" || message == "known transaction" {
        return true;
    }
    message
        .strip_prefix("known transaction: ")
        .is_some_and(|hash| names(hash, tx_hash))
}

/// [`is_already_known`] on the `RPC error <code>: <message>` text `RpcClient` builds.
/// Error text carrying a ` data=` suffix does not match.
pub(crate) fn is_already_known_rpc_error(text: &str, tx_hash: &B256) -> bool {
    text.strip_prefix("RPC error ")
        .and_then(|rest| rest.split_once(": "))
        .is_some_and(|(code, message)| {
            code.parse::<i64>().is_ok() && is_already_known(message, tx_hash)
        })
}

fn names(text: &str, tx_hash: &B256) -> bool {
    let digits = text.strip_prefix("0x").unwrap_or(text);
    digits.len() == 64 && hex::decode(digits).is_ok_and(|bytes| bytes == tx_hash.as_slice())
}

#[cfg(test)]
mod tests {
    use super::*;

    const HASH: B256 = B256::repeat_byte(0xab);

    fn rpc(message: &str) -> String {
        format!("RPC error -32000: {message}")
    }

    #[test]
    fn whole_known_messages_match() {
        for message in [
            "already known".to_string(),
            " Already Known ".to_string(),
            "Known transaction".to_string(),
            format!("known transaction: {}", hex::encode(HASH)),
            format!("known transaction: {HASH:#x}"),
            format!("Known transaction: {}", hex::encode(HASH).to_uppercase()),
        ] {
            assert!(is_already_known(&message, &HASH), "{message}");
            assert!(
                is_already_known_rpc_error(&rpc(&message), &HASH),
                "{message}"
            );
        }
    }

    #[test]
    fn messages_that_only_contain_the_phrase_do_not_match() {
        let hash = hex::encode(HASH);
        for message in [
            "unknown transaction".to_string(),
            format!("unknown transaction: {hash}"),
            "execution reverted: already known".to_string(),
            "already known: gas price below minimum".to_string(),
            "Post \"https://sequencer.test\": already known".to_string(),
            "known transactions exceeded".to_string(),
            format!("known transaction: {}", hex::encode(B256::repeat_byte(9))),
            format!("known transaction: {hash}; nonce too low"),
            format!("known transaction: {}", &hash[..62]),
            "known transaction: zz".to_string(),
            String::new(),
        ] {
            assert!(!is_already_known(&message, &HASH), "{message}");
            assert!(
                !is_already_known_rpc_error(&rpc(&message), &HASH),
                "{message}"
            );
        }
    }

    #[test]
    fn rpc_error_text_must_be_the_client_format_without_data() {
        let legacy = format!("known transaction: {}", hex::encode(HASH));
        for text in [
            "already known".to_string(),
            "RPC error x: already known".to_string(),
            rpc("already known data=0x01"),
            rpc(&format!("{legacy} data=0x01")),
        ] {
            assert!(!is_already_known_rpc_error(&text, &HASH), "{text}");
        }
        assert!(is_already_known_rpc_error(
            "RPC error 3: already known",
            &HASH
        ));
    }
}
