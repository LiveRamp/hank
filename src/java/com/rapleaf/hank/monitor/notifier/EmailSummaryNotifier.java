/**
 *  Copyright 2011 Rapleaf
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

package com.rapleaf.hank.monitor.notifier;

import com.rapleaf.hank.monitor.notification.NotificationFormatter;
import org.apache.log4j.Logger;

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

  private static final Logger LOG = Logger.getLogger(EmailSummaryNotifier.class);
  private static final int EMAIL_SUMMARY_FREQUENCY = 60 * 1000; // 1 minute in ms

  private final String name;
  private final Set<String> emailTargets;
  private final Properties emailSessionProperties = new Properties();
  private final Thread notifierThread;
  private final NotificationFormatter notificationFormatter;

  private final List<Notification> notifications = new ArrayList<Notification>();

  public EmailSummaryNotifier(String name,
                              Set<String> emailTargets,
                              String smtpHost,
                              NotificationFormatter notificationFormatter) {
    this.name = name;
    this.emailTargets = emailTargets;
    this.emailSessionProperties.put("mail.smtp.host", smtpHost);
    this.notificationFormatter = notificationFormatter;
    this.notifierThread = new Thread(new Runnable() {
      @Override
      public void run() {
        while (true) {
          synchronized (notifications) {
            if (notifications.size() > 0) {
              notifySummary();
            }
          }
          try {
            Thread.sleep(EMAIL_SUMMARY_FREQUENCY);
            // Interrup to stop the notification loop
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
    StringBuilder summary = new StringBuilder();
    synchronized (notifications) {
      LOG.info("Sending Monitor email to " + emailTargets + " containing " + notifications.size() + " notifications.");
      for (Notification notification : notifications) {
        summary.append(notification.format(notificationFormatter));
        summary.append('\n');
      }
      notifications.clear();
    }
    try {
      sendEmails(emailTargets, summary.toString());
    } catch (Exception e) {
      throw new RuntimeException("Exception while sending email notification.", e);
    }
  }

  private void sendEmails(Set<String> emailTargets, String body) throws IOException, InterruptedException {
    Session session = Session.getDefaultInstance(emailSessionProperties);
    Message message = new MimeMessage(session);
    try {
      message.setSubject("Hank: " + name);
      for (String emailTarget : emailTargets) {
        message.addRecipient(Message.RecipientType.TO, new InternetAddress(emailTarget));
      }
      message.setText(body);
      Transport.send(message);
    } catch (MessagingException e) {
      throw new RuntimeException("Failed to send notification email.", e);
    }
  }

  @Override
  public void notify(Notification notification) {
    synchronized (notifications) {
      notifications.add(notification);
    }
  }
}
