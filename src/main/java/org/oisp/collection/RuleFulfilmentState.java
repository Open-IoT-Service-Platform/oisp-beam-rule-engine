package org.oisp.collection;

import java.io.Serializable;
import java.util.ArrayList;

public class RuleFulfilmentState implements Serializable {
    private String id;
    private String accountId;
    private boolean fulfilled;
    private ArrayList<Integer> condFulfillment;

    public RuleFulfilmentState() {
        fulfilled = false;
        condFulfillment = new ArrayList<>();
    }
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getAccountId() {
        return accountId;
    }

    public void setAccountId(String accountId) {
        this.accountId = accountId;
    }

    public boolean isFulfilled() {
        return fulfilled;
    }

    public void setFulfilled(boolean fulfilled) {
        this.fulfilled = fulfilled;
    }

    public ArrayList<Integer> getCondFulfillment() {
        return condFulfillment;
    }

    public void setCondFulfillment(ArrayList<Integer> condFulfillment) {
        this.condFulfillment = condFulfillment;
    }
}
