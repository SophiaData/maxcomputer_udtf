package io.sophiadata.parse.field;

import com.aliyun.odps.udf.UDFException;
import com.aliyun.odps.udf.UDTF;
import com.aliyun.odps.udf.annotation.Resolve;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

@Resolve({"string->string,string"}) // 假设输出两个字符串列，分别为键名和键值
public class JsonUDTF extends UDTF {

    private static final Logger LOG = LoggerFactory.getLogger(JsonUDTF.class);

    private final transient ObjectMapper objectMapper = new ObjectMapper();
    private final List<Object[]> bufferedRows = new ArrayList<>(); // 用来暂存数据

    @Override
    public void process(Object[] args) throws UDFException {
        if (args[0] == null) {
            return; // 输入为空则直接返回
        }
        String jsonData = (String) args[0];
        try {
            JsonNode rootNode = objectMapper.readTree(jsonData);
            traverseAndEmit(rootNode, ""); // 初始调用，父键为空
            // 发射暂存的数据
            for (Object[] row : bufferedRows) {
                forward(row);
            }
            bufferedRows.clear(); // 清空缓存
        } catch (Exception e) {
            throw new UDFException("Error occurred while parsing JSON data: " + e.getMessage());
        }
    }

    private void traverseAndEmit(JsonNode node, String parentKey) throws UDFException {
        if (node.isObject()) {
            Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> entry = fields.next();
                String currentKey = entry.getKey();
                JsonNode childNode = entry.getValue();
                traverseAndEmit(
                        childNode, parentKey.isEmpty() ? currentKey : parentKey + "." + currentKey);
            }
        } else if (node.isArray()) {
            for (int i = 0; i < node.size(); i++) {
                JsonNode arrayElement = node.get(i);
                traverseAndEmit(arrayElement, parentKey + "[" + i + "]");
            }
        } else { // 基本类型
            bufferedRows.add(new Object[] {parentKey, node.asText()}); // 缓存数据，稍后统一发射
        }
    }
    // 新增模拟测试辅助方法
    public List<Map.Entry<String, String>> testTraverseAndCollect(String jsonData)
            throws UDFException {
        List<Map.Entry<String, String>> collectedData = new ArrayList<>();
        try {
            JsonNode rootNode = objectMapper.readTree(jsonData);
            traverseAndCollect(rootNode, "", collectedData);
        } catch (JsonProcessingException e) {
            throw new UDFException("Error processing JSON data");
        }
        return collectedData;
    }

    // 辅助方法，用于收集解析结果
    private void traverseAndCollect(
            JsonNode node, String parentKey, List<Map.Entry<String, String>> collector) {
        if (node.isObject()) {
            Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> entry = fields.next();
                String currentKey = entry.getKey();
                JsonNode childNode = entry.getValue();
                traverseAndCollect(
                        childNode,
                        parentKey.isEmpty() ? currentKey : parentKey + "." + currentKey,
                        collector);
            }
        } else if (node.isArray()) {
            for (int i = 0; i < node.size(); i++) {
                JsonNode arrayElement = node.get(i);
                traverseAndCollect(arrayElement, parentKey + "[" + i + "]", collector);
            }
        } else { // 基本类型
            collector.add(new AbstractMap.SimpleEntry<>(parentKey, node.asText()));
        }
    }

    //
    public static void main(String[] args) {
        try {
            JsonUDTF udtf = new JsonUDTF();
            String jsonData =
                    "{\"name\":\"Alice\",\"age\":25,\"city\":\"Beijing\",\"hobbies\":[\"reading\",\"swimming\"]}";
            List<Map.Entry<String, String>> results =
                    udtf.testTraverseAndCollect(jsonData); // 更新类型匹配
            // 打印或验证results...
            for (Map.Entry<String, String> entry : results) {
                System.out.println(entry.getKey() + ": " + entry.getValue()); // 打印键值对
            }
        } catch (Exception e) {
            LOG.error("Error occurred while running JsonUDTF", e);
        }
    }
}
