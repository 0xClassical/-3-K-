//! Local, user-supplied monitored wallets; no production wallet list is shipped.
use std::collections::HashSet;
use std::path::Path;
use std::sync::OnceLock;

static TARGETS: OnceLock<HashSet<String>> = OnceLock::new();

/// Only offline unit tests use this public dummy address, never production startup.
#[cfg(test)]
pub const TARGET_BUYERS: &[&str] = &["0x1111111111111111111111111111111111111111"];

pub fn parse_targets(text: &str) -> Result<HashSet<String>, String> {
    let addresses: Vec<String> =
        serde_json::from_str(text).map_err(|_| "监控地址配置必须是 JSON 字符串数组".to_string())?;
    if addresses.is_empty() {
        return Err("监控地址列表为空，请填入你自己的地址后启动".into());
    }
    if addresses.len() > 10_000 {
        return Err("监控地址列表最多 10000 项".into());
    }
    let mut targets = HashSet::new();
    for address in addresses {
        let address = address.trim().to_ascii_lowercase();
        if address.len() != 42
            || !address.starts_with("0x")
            || !address[2..].bytes().all(|b| b.is_ascii_hexdigit())
            || address == crate::config::ZERO_ADDRESS
        {
            return Err("列表中有无效地址（需要非零 0x + 40 位十六进制地址）".into());
        }
        if !targets.insert(address) {
            return Err("监控地址重复（忽略大小写）".into());
        }
    }
    Ok(targets)
}

pub fn initialize(path: &Path) -> Result<usize, String> {
    let size = std::fs::metadata(path)
        .map_err(|_| "监控地址配置不存在或不可读，请填写 config/targets.local.json".to_string())?
        .len();
    if size > 1_000_000 {
        return Err("监控地址配置超过 1 MB，拒绝读取".into());
    }
    let text = std::fs::read_to_string(path)
        .map_err(|_| "无法读取监控地址配置；请复制 config/targets.example.json 为 config/targets.local.json 并填入自己的地址".to_string())?;
    let targets = parse_targets(&text)?;
    let count = targets.len();
    TARGETS
        .set(targets)
        .map_err(|_| "监控地址只允许在启动时初始化一次".to_string())?;
    Ok(count)
}

pub fn target_count() -> usize {
    #[cfg(test)]
    {
        TARGET_BUYERS.len()
    }
    #[cfg(not(test))]
    {
        TARGETS.get().map_or(0, HashSet::len)
    }
}

pub fn is_target_buyer(wallet: &str) -> bool {
    #[cfg(test)]
    {
        TARGET_BUYERS
            .iter()
            .any(|target| target.eq_ignore_ascii_case(wallet))
    }
    #[cfg(not(test))]
    {
        TARGETS
            .get()
            .is_some_and(|targets| targets.contains(&wallet.to_ascii_lowercase()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn local_targets_are_validated_without_a_production_list() {
        let list = parse_targets(r#"["0xabcdefabcdefabcdefabcdefabcdefabcdefabcd"]"#).unwrap();
        assert!(list.contains("0xabcdefabcdefabcdefabcdefabcdefabcdefabcd"));
        assert!(parse_targets("[]").is_err());
        assert!(parse_targets(r#"["not-an-address"]"#).is_err());
        assert!(parse_targets(r#"["0x0000000000000000000000000000000000000000"]"#).is_err());
        assert!(parse_targets(r#"["0xABCDEFABCDEFABCDEFABCDEFABCDEFABCDEFABCD","0xabcdefabcdefabcdefabcdefabcdefabcdefabcd"]"#).is_err());
        assert!(parse_targets(r#"{"targets":[]}"#).is_err());
        assert!(is_target_buyer(TARGET_BUYERS[0]));
        assert!(!is_target_buyer(crate::config::ZERO_ADDRESS));
    }
    #[test]
    fn large_lists_and_non_string_items_fail_closed() {
        assert!(
            parse_targets(&serde_json::to_string(&vec![TARGET_BUYERS[0]; 10_001]).unwrap())
                .is_err()
        );
        assert!(parse_targets("[null, 1]").is_err());
        assert!(parse_targets("[\"0x１２３４\"]").is_err());
    }
}
