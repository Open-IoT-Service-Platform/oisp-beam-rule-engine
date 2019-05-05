package org.oisp.collection.subCollections;

import java.io.Serializable;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;

public class NormalizedStatisticsValues implements Serializable {
    private Double accumValues; //accumulated values of samples
    private Double accumSquared; // all squared
    private SortedMap<Long, Double> observationsInWindow; // observations which matter

    public NormalizedStatisticsValues() {
        accumValues = 0.0;
        accumSquared = 0.0;
        observationsInWindow = new TreeMap<Long, Double>();
    }


    public NormalizedStatisticsValues(Double avg, Double sigmaSquared, Long valueTS, Double value) {
        this.accumValues = avg;
        this.accumSquared = sigmaSquared;
        observationsInWindow = new TreeMap<Long, Double>();
        observationsInWindow.put(valueTS, value);
    }

    public NormalizedStatisticsValues(NormalizedStatisticsValues other) {
        this.accumValues = other.accumValues;
        this.accumSquared = other.accumSquared;
        this.observationsInWindow = other.observationsInWindow;
    }

    //why not using putall?
    //Reason, if other contains duplicate keys, accumValues and sigmasquared would be wrong
    public void add(NormalizedStatisticsValues other) {
        for (SortedMap.Entry<Long, Double> entry : other.getObservationsInWindow().entrySet()) {
            if (observationsInWindow.get(entry.getKey()) == null) {
                accumValues += other.accumValues;
                accumSquared += other.accumSquared;
                observationsInWindow.put(entry.getKey(), entry.getValue());
            }
        }
    }

    public Double getAverage() {
        return accumValues / Double.valueOf(observationsInWindow.size());
    }

    public Double getStdDev() {
        Double average = getAverage();
        Double stddev = -1.0;
        Optional<Double> stddevOpt = observationsInWindow.entrySet().stream().map(v -> v.getValue())
                .reduce((t, acc) -> acc + (t - average) * (t - average));
        if (stddevOpt.isPresent()) {
            stddev = Math.sqrt(stddevOpt.get()) / Double.valueOf(getNumSamples());
        }
        return stddev;
    }

    public Integer getNumSamples() {
        return observationsInWindow.size();
    }

    public void remove(Long timeStamp) {
        Double value = observationsInWindow.get(timeStamp);
        if (value == null) {
            return;
        }
        observationsInWindow.remove(timeStamp);
        accumValues -= value;
        accumSquared -= value * value;

    }

    public Long getTimeWindowStart() {
        if (observationsInWindow.isEmpty()) {
            return -1L;
        }
        return observationsInWindow.firstKey();
    }

    public Long getTimeWindowEnd() {
        if (observationsInWindow.isEmpty()) {
            return -1L;
        }
        return observationsInWindow.lastKey();
    }

    public SortedMap<Long, Double> getObservationsInWindow() {
        return observationsInWindow;
    }

    public void setObservationsInWindow(SortedMap<Long, Double> observationsInWindow) {
        this.observationsInWindow = observationsInWindow;
    }

    public Double getAccumValues() {
        return accumValues;
    }

    public void setAccumValues(Double accumValues) {
        this.accumValues = accumValues;
    }

    public Double getAccumSquared() {
        return accumSquared;
    }

    public void setAccumSquared(Double accumSquared) {
        this.accumSquared = accumSquared;
    }

}
