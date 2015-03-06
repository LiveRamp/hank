/**
 *  Copyright 2011 LiveRamp
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.liveramp.hank.monitor.notifier.email;

import com.liveramp.hank.monitor.notification.NotificationFormatter;
import com.liveramp.hank.monitor.notifier.Notification;
import com.liveramp.hank.monitor.notifier.Notifier;
import org.slf4j.Logger; import org.slf4j.LoggerFactory;

import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;

public class EmailSummaryNotifier implements Notifier {

  private static final Logger LOG = LoggerFactory.getLogger(EmailSummaryNotifier.class);
  private static final int EMAIL_SUMMARY_FREQUENCY = 60 * 1000; // 1 minute in ms

  private final String name;
  private final String emailFrom;
  private final Set<String> emailTargets;
  private final Properties emailSessionProperties = new Properties();
  private final Thread notifierThread;
  private final NotificationFormatter notificationFormatter;

  private final List<Notification> notifications = new ArrayList<Notification>();

  public EmailSummaryNotifier(String name,
                              String emailFrom,
                              Set<String> emailTargets,
                              String smtpHost,
                              NotificationFormatter notificationFormatter) {
    this.name = name;
    this.emailFrom = emailFrom;
    this.emailTargets = emailTargets;
    this.emailSessionProperties.put("mail.smtp.host", smtpHost);
    this.notificationFormatter = notificationFormatter;
    this.notifierThread = new Thread(new Runnable() {
      @Override
      public void run() {
        while (true) {
          notifySummary();
          try {
            Thread.sleep(EMAIL_SUMMARY_FREQUENCY);
            // Interrupt to stop the notification loop
          } catch (InterruptedException e) {
            // Notify what is left before stopping
            notifySummary();
            break;
          }
        }
      }
    }, this.getClass().getSimpleName() + " notifier thread");
    notifierThread.start();
  }

  @Override
  public void stop() {
    if (notifierThread != null) {
      notifierThread.interrupt();
      try {
        notifierThread.join();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private void notifySummary() {
    synchronized (notifications) {
      if (notifications.size() > 0) {
        StringBuilder summary = new StringBuilder();
        summary.append("<html><body>");
        LOG.info("Sending Monitor email to " + emailTargets + " containing " + notifications.size() + " notifications.");
        for (Notification notification : notifications) {
          summary.append(notification.format(notificationFormatter));
          summary.append("<br />");
        }
        notifications.clear();
        summary.append("</body></html>");
        try {
          sendEmails(emailTargets, summary.toString());
        } catch (Exception e) {
          throw new RuntimeException("Exception while sending email notification.", e);
        }
      }
    }
  }

  private void sendEmails(Set<String> emailTargets, String body) throws IOException, InterruptedException {
    Session session = Session.getDefaultInstance(emailSessionProperties);
    Message message = new MimeMessage(session);
    try {
      message.setSubject("Hank monitor: " + name);
      message.setFrom(new InternetAddress(emailFrom));
      for (String emailTarget : emailTargets) {
        message.addRecipient(Message.RecipientType.TO, new InternetAddress(emailTarget));
      }
      message.setContent(body, "text/html");
      Transport.send(message);
    } catch (MessagingException e) {
      throw new RuntimeException("Failed to send notification email.", e);
    }
  }

  @Override
  public void doNotify(Notification notification) {
    synchronized (notifications) {
      notifications.add(notification);
    }
  }
}
