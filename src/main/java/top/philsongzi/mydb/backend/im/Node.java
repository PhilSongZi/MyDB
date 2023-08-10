package top.philsongzi.mydb.backend.im;

import top.philsongzi.mydb.backend.common.SubArray;
import top.philsongzi.mydb.backend.dm.DataManager;
import top.philsongzi.mydb.backend.utils.Parser;

import java.util.Arrays;

/**
 * Node 类，用于表示 B+ 树的节点。其结构：
 * [LeafFlag][KeyNumber][SiblingUid]
 * [Son0][Key0][Son1][Key1]...[SonN][KeyN]
 * 分别表示：是否为叶子节点、键值对的数量、兄弟节点的 UID。
 * 穿插的子节点1-N，最后一个KeyN始终为MAX_VALUE，方便查找。
 *
 * @author 小子松
 * @since 2023/8/10
 */
public class Node {

    // Node 结构的常量，后面的方法会根据偏移量来获取数据
    static final int IS_LEAF_OFFSET = 0;
    static final int NO_KEYS_OFFSET = IS_LEAF_OFFSET + 1;
    static final int SIBLING_OFFSET = NO_KEYS_OFFSET + 2;
    static final int NODE_HEADER_SIZE = SIBLING_OFFSET + 8;
    static final int BALANCE_NUMBER = 32;
    static final int NODE_SIZE = NODE_HEADER_SIZE + (2 * 8) * (BALANCE_NUMBER * 2 + 2);

    // 需要维护的引用：
    BPlusTree tree;  // B+ 树
    DataManager dm;  // 数据管理器
    SubArray raw;    // 节点的原始数据
    long uid;        // 节点的 UID

    // 设置节点是否为叶子节点，因为Node结构的第一个字节为 是否为叶子节点的标志位
    static void setRawIsLeaf(SubArray raw, boolean isLeaf) {
        if (isLeaf) {
            raw.raw[raw.start + IS_LEAF_OFFSET] = (byte) 1;
        } else {
            raw.raw[raw.start + IS_LEAF_OFFSET] = (byte) 0;
        }
    }

    // 获取节点是否为叶子节点
    static boolean getRawIfLeaf(SubArray raw) {
        return raw.raw[raw.start + IS_LEAF_OFFSET] == (byte) 1;
    }

    // 设置节点的键值对数量
    static void setRawNoKeys(SubArray raw, int noKeys) {
        System.arraycopy(Parser.short2Byte((short) noKeys), 0, raw.raw, raw.start + NO_KEYS_OFFSET, 2);
    }

    // 获取节点的键值对数量
    static int getRawNoKeys(SubArray raw) {
        return (int) Parser.parseShort(
                // 从 raw.raw 数组中截取出键值对数量的字节数组，然后解析为 short 类型
                Arrays.copyOfRange(raw.raw, raw.start + NO_KEYS_OFFSET, raw.start + NO_KEYS_OFFSET + 2)
        );
    }
}
