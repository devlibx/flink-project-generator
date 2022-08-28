### What this job does

This is a template job which listens to event and generates events if some event state is not completed

e.g.
We have a example where order event gets first event with status=INIT. Now we need to trigger event f we did not
get status=Completed for 30 second

This Job has a rules return in Drools:

1. Filter Input Stream - Filter a input stream skip any unwanted request
    1. in this example we only use status=COMPLETED and status=INIT events. Other events are ignored
2. Order Initiated - When status with INIT is received
    1. Internally we store this to flink internal state and run a timer defined in "retain-state-expiry-in-sec"
3. Order Expire Trigger - If we did not get status=COMPLETED in 2 sec
    1. Here we send a alert event
    2. This output data can be created in drools rule engine itself

