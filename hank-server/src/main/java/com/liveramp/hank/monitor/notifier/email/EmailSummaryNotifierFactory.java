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

import com.liveramp.hank.config.InvalidConfigurationException;
import com.liveramp.hank.monitor.notification.EmailNotificationFormatter;
import com.liveramp.hank.monitor.notifier.AbstractNotifierFactory;
import com.liveramp.hank.monitor.notifier.Notifier;
import com.liveramp.hank.monitor.notifier.NotifierFactory;

import java.util.HashSet;
import java.util.Map;

public class EmailSummaryNotifierFactory extends AbstractNotifierFactory implements NotifierFactory {

  private static final String EMAIL_NOTIFICATION_FROM_KEY = "email_notification_from";
  private static final String EMAIL_NOTIFICATION_TARGETS_KEY = "email_notification_targets";
  private static final String SMTP_HOST_KEY = "smtp_host";

  @Override
  public void validate(Map<String, Object> configuration) throws InvalidConfigurationException {
    getRequiredString(configuration, EMAIL_NOTIFICATION_TARGETS_KEY);
    getRequiredString(configuration, SMTP_HOST_KEY);
  }

  @Override
  public Notifier createNotifier(Map<String, Object> configuration,
                                 String name,
                                 String webUiUrl) {
    String emailNotificationFrom = getString(configuration, EMAIL_NOTIFICATION_FROM_KEY);
    HashSet<String> emailNotificationTargets = new HashSet<String>();
    String[] emailNotificationTargetsTokens = getString(configuration, EMAIL_NOTIFICATION_TARGETS_KEY).split(",");
    for (String emailNotificationTarget : emailNotificationTargetsTokens) {
      emailNotificationTargets.add(emailNotificationTarget.trim());
    }
    return new EmailSummaryNotifier(name, emailNotificationFrom,
        emailNotificationTargets,
        getString(configuration, SMTP_HOST_KEY),
        new EmailNotificationFormatter(webUiUrl));
  }
}
