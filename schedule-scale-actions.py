'''
Created on 20/03/2013

@author: mac
'''

from boto.ec2.autoscale import connect_to_region

from datetime import date
import datetime

# AWS credentials
AWS_KEY    = 'AKIAI52ERUQP3UL5K2QA' 
AWS_SECRET = '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'

# Scaling group
SCALING_GROUP = "Riak-Ireland-RiakServerGroup-1AZTEGQHS7E5U"

conn = connect_to_region('eu-west-1', aws_access_key_id=AWS_KEY, aws_secret_access_key=AWS_SECRET)

today = date.today()
now   = datetime.datetime.now()

shutdown_time = datetime.datetime(today.year, today.month, today.day, 16 - 1, 00)
startup_time  = datetime.datetime(today.year, today.month, today.day, now.hour - 1, now.minute + 3)

# Scheduling tasks
conn.create_scheduled_group_action(as_group=SCALING_GROUP, name="SHUTDOWN", time=shutdown_time, desired_capacity=0, min_size=0, max_size=2)
conn.create_scheduled_group_action(as_group=SCALING_GROUP, name="STARTUP", time=startup_time, desired_capacity=2, min_size=2, max_size=2)

# Printing results
scheduled_actions = conn.get_all_scheduled_actions(as_group=SCALING_GROUP)

for action in scheduled_actions:
    print "{0}: {1} desired={2} min={3} max={4}".format(action.name, action.time, action.desired_capacity, action.min_size, action.max_size)
