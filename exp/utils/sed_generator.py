from typing import Optional


def sed_key_len(config_path: str, key_size: int):
    old_key_code   = "^constexpr uint32_t keyLen = .*"
    new_key_code   = f"constexpr uint32_t keyLen = {key_size};"
    return f"sed -i 's/{old_key_code}/{new_key_code}/g' {config_path}"


def sed_val_len(config_path: str, value_size: int):
    old_val_code   = "^constexpr uint32_t simulatedValLen =.*"
    new_val_code   = f"constexpr uint32_t simulatedValLen = {value_size};"
    return f"sed -i 's/{old_val_code}/{new_val_code}/g' {config_path}"


def sed_cache_size(config_path: str, cache_size: int):
    old_cache_code = "^constexpr int kIndexCacheSize = .*"
    new_cache_node = f"constexpr int kIndexCacheSize = {cache_size};"
    return f"sed -i 's/{old_cache_code}/{new_cache_node}/g' {config_path}"


def sed_MN_num(config_path: str, MN_num: int):
    old_MN_code = "^#define MEMORY_NODE_NUM .*"
    new_MN_node = f"#define MEMORY_NODE_NUM {MN_num}"
    return f"sed -i 's/{old_MN_code}/{new_MN_node}/g' {config_path}"


def sed_span_size(config_path: str, span_size: int):  # only for Sherman
    old_span_code = "^constexpr int spanSize = .*"
    new_span_node = f"constexpr int spanSize = {span_size};"
    return f"sed -i 's/{old_span_code}/{new_span_node}/g' {config_path}"

def sed_node_type_num(config_path: str, node_type_num: int):
    old_type_num_code   = "^constexpr uint32_t adaptiveNodeTypeNum = .*"
    new_type_num_code   = f"constexpr uint32_t adaptiveNodeTypeNum = {node_type_num};"
    return f"sed -i 's/{old_type_num_code}/{new_type_num_code}/g' {config_path}"

def sed_local_lock_num(config_path: str, local_lock_num_M: int):
    old_local_lock_code   = "^constexpr uint64_t kLocalLockNum = .*"
    new_local_lock_code   = f"constexpr uint64_t kLocalLockNum = {local_lock_num_M};"
    return f"sed -i 's/{old_local_lock_code}/{new_local_lock_code}/g' {config_path}"

def generate_sed_cmd(config_path: str, is_Btree: bool, key_size: int, value_size: int, cache_size: int, MN_num: int, span_size: Optional[int] = None, local_lock_num: Optional[int] = 4 * 1024 * 1024):
    cmd = f"{sed_key_len(config_path, key_size)} && {sed_val_len(config_path, value_size)} && {sed_cache_size(config_path, cache_size)} && {sed_MN_num(config_path, MN_num)} && {sed_local_lock_num(config_path, local_lock_num)}"
    if local_lock_num > 0:
        cmd += f"&& {sed_span_size(config_path, span_size)}"
    if is_Btree:  # change span size for Sherman
        assert(span_size is not None)
        cmd += f"&& {sed_span_size(config_path, span_size)}"
    return cmd
