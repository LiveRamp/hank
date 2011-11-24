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

import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class EmailSummaryNotifier implements Notifier {

  private static final Logger LOG = Logger.getLogger(EmailSummaryNotifier.class);

  private static final int EMAIL_SUMMARY_FREQUENCY = 60 * 1000; // 1 minute in ms
  private final Set<String> emailTargets;
  private final Thread notifierThread;

  private final List<Notification> notifications = new ArrayList<Notification>();

  public EmailSummaryNotifier(Set<String> emailTargets) {
    this.emailTargets = emailTargets;
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
      for (Notification notification : notifications) {
        summary.append(notification.format());
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
    LOG.info("Sending Monitor email to " + emailTargets.size() + " targets: " + emailTargets + " Email body: " + body);
    for (String emailTarget : emailTargets) {
      String[] command = {"/bin/mail", "-s", "Hank-Notification", emailTarget};
      Process process = Runtime.getRuntime().exec(command);
      OutputStreamWriter writer = new OutputStreamWriter(process.getOutputStream());
      writer.write(body);
      writer.close();
      process.getOutputStream().close();
      process.waitFor();
    }
  }

  @Override
  public void notify(Notification notification) {
    synchronized (notifications) {
      notifications.add(notification);
    }
  }
}
