package org.oisp.transformation;

import org.apache.beam.sdk.transforms.DoFn;
import org.oisp.apiclients.DashboardConfigProvider;
import org.oisp.apiclients.InvalidDashboardResponseException;
import org.oisp.apiclients.alerts.AlertsApi;
import org.oisp.apiclients.alerts.DashboardAlertsApi;
import org.oisp.collection.Observation;
import org.oisp.collection.Rule;
import org.oisp.collection.RulesWithObservation;
import org.oisp.conf.Config;
import org.oisp.utils.LogHelper;
import org.slf4j.Logger;

import java.util.Arrays;
import java.util.Optional;

public class SendAlertFromRule extends DoFn<Rule, Byte> {


    private static final Logger LOG = LogHelper.getLogger(PersistRulesTask.class);
    private final AlertsApi alertsApi;

    public SendAlertFromRule(Config userConfig) {
        this(new DashboardAlertsApi(new DashboardConfigProvider(userConfig)));
    }

    public SendAlertFromRule(AlertsApi alertsApi) {
        this.alertsApi = alertsApi;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {

        Rule rule = c.element();
        //find the observation which triggered the Rule
        //Which is the Observation for which condition is true and which has the most recent timestamp
        Optional<Observation> obsOpt = rule.getConditions().stream().filter(rc -> rc.getFulfilled())
                .map(rc -> rc.getObservation())
                .reduce((o, accum) -> o.getOn() > accum.getOn() ? o : accum);
        if (obsOpt.isPresent()) {
            Observation obs = obsOpt.get();
            RulesWithObservation rWO = new RulesWithObservation(obs, Arrays.asList(rule));
            try {
                alertsApi.pushAlert(Arrays.asList(rWO));
            } catch (InvalidDashboardResponseException e) {
                LOG.error("Unable to send alerts for fulfilled rules", e);
            }
        }

        //getContext().output(new Message(message, now()));
    }
}
