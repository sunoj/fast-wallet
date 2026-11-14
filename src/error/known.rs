// Node answers proving a pool already holds the exact signed bytes of a send.
// Exports exact-match predicates for raw JSON-RPC messages and RpcClient's error text.

/// True when an `eth_sendRawTransaction` error message says the node already holds this
/// transaction. Pools key that answer by transaction hash, which commits to the signed
/// bytes (a different transaction at the same nonce is "replacement transaction
/// underpriced"), so the send is in that node's pool. The whole message must match:
/// "already known" (geth, op-geth, reth), "known transaction: <hash>" (legacy geth) or
/// "Known transaction" (Besu). A message that only contains the phrase, such as a revert
/// reason or a compound proxy error, does not match.
pub(crate) fn is_already_known(message: &str) -> bool {
    let message = message.trim().to_ascii_lowercase();
    message == "already known"
        || message == "known transaction"
        || message.starts_with("known transaction: ")
}

/// [`is_already_known`] on the `RPC error <code>: <message>` text `RpcClient` builds.
/// Error text carrying a ` data=` suffix does not match.
pub(crate) fn is_already_known_rpc_error(text: &str) -> bool {
    text.strip_prefix("RPC error ")
        .and_then(|rest| rest.split_once(": "))
        .is_some_and(|(code, message)| code.parse::<i64>().is_ok() && is_already_known(message))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn whole_known_messages_match() {
        for message in [
            "already known",
            " Already Known ",
            "Known transaction",
            "known transaction: 5d1a9e",
        ] {
            assert!(is_already_known(message), "{message}");
            assert!(is_already_known_rpc_error(&format!(
                "RPC error -32000: {message}"
            )));
        }
    }

    #[test]
    fn messages_that_only_contain_the_phrase_do_not_match() {
        for message in [
            "unknown transaction",
            "unknown transaction: 5d1a9e",
            "execution reverted: already known",
            "already known: gas price below minimum",
            "Post \"https://sequencer.test\": already known",
            "known transactions exceeded",
            "",
        ] {
            assert!(!is_already_known(message), "{message}");
            assert!(!is_already_known_rpc_error(&format!(
                "RPC error -32000: {message}"
            )));
        }
    }

    #[test]
    fn rpc_error_text_must_be_the_client_format() {
        assert!(!is_already_known_rpc_error("already known"));
        assert!(!is_already_known_rpc_error("RPC error x: already known"));
        assert!(!is_already_known_rpc_error(
            "RPC error -32000: already known data=0x01"
        ));
        assert!(is_already_known_rpc_error("RPC error 3: already known"));
    }
}
