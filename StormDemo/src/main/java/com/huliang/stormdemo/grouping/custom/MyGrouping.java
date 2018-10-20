package com.huliang.stormdemo.grouping.custom;

import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.task.WorkerTopologyContext;

import java.util.ArrayList;
import java.util.List;

/**
 * @author huliang
 * @date 2018/10/20 21:55
 */
public class MyGrouping implements CustomStreamGrouping {

    private List<Integer> targetTasks;

    // 预备,接收context、数据流stream和接收tuple数据的taskId集合
    public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks) {
        this.targetTasks = targetTasks;
    }

    // 返回tuple数据要发送的taskId: 选择前一半的task,发送数据
    public List<Integer> chooseTasks(int taskId, List<Object> values) {
        List<Integer> emitTaskIds = new ArrayList<Integer>();   // 要发送的id
        for (int i = 0; i <= targetTasks.size() / 2; i++)
            emitTaskIds.add(targetTasks.get(i));
        return emitTaskIds;
    }
}
