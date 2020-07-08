package com.aliyun.odps.datacarrier.taskscheduler.event;

import java.util.Objects;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.dingtalk.api.DefaultDingTalkClient;
import com.dingtalk.api.DingTalkClient;
import com.dingtalk.api.request.OapiRobotSendRequest;

public class MmaEventSenderDingTalkImpl implements MmaEventSender {
  private static final Logger LOG = LogManager.getLogger(MmaEventSenderDingTalkImpl.class);

  private DingTalkClient dingTalkClient;

  public MmaEventSenderDingTalkImpl(String webhookUrl) {
    dingTalkClient = new DefaultDingTalkClient(Objects.requireNonNull(webhookUrl));
  }

  @Override
  public void send(BaseMmaEvent event) {
    OapiRobotSendRequest request = new OapiRobotSendRequest();
    request.setMsgtype("markdown");
    OapiRobotSendRequest.Markdown markdown = new OapiRobotSendRequest.Markdown();
    markdown.setTitle(event.getType().name());
    markdown.setText(event.toString());
    request.setMarkdown(markdown);
    try {
      dingTalkClient.execute(request);
      LOG.info("Event sent, id: {}", event.getId());
    } catch (Exception e) {
      LOG.warn("Failed to send dingtalk message", e);
    }
  }
}
