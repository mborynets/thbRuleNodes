# rule-node-examples
Examples of custom Rule Nodes for ThingsBoard contribution guide

Unit tests examples for custom rule nodes added as well

To build the project run `mvn clean install`

To send jar file to ThB WM run `scp target/rule-engine-1.0.0-custom-nodes.jar manager@10.160.180.10:/tmp/`

Execute the following command to copy jar-file to ThingsBoard extensions: `sudo cp rule-engine-1.0.0-custom-nodes.jar /usr/share/thingsboard/extensions/`

Next, execute the following to change the owner to ThingsBoard: `sudo chown thingsboard:thingsboard /usr/share/thingsboard/extensions/*`

Restart ThingsBoard service: `sudo service thingsboard restart`