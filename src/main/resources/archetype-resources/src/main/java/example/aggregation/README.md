### How to test your DRL logic

It is difficult to write a DRL file in IDE. To help, I have created test case to test the
logic for DRL file.

Test to run: ```aggregation/DroolTest.java```
DRL Logic: ```TestDroolsLogic.java```

You write your logic in IDE in ```TestDroolsLogic.java``` file and test with ```DroolTest.java```.
You will have to modify the test and DRL logic as you need.

---

##### Once your logic is tested, then you can transfer to your DRL file

Once you are done, you can copy the logic of ```TestDroolsLogic.java``` to tour DRL
file. For me I did it in ```test_aggregate_sample_rule.drl```.

Finally, you can test your DRL file with ```CustomProcessorFunctionTest.java```

---

### How to run

From IDE you set ```--config <Path to aggregation.yaml>``` and run
with ```io.github.devlibx.flink.example.aggregation.Main```

Make sure you have access to DDB table - in this example I create a table "harish-test" with
id: string
sub_key: string