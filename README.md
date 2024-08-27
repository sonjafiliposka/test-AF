# test-AF

This is a public repository that contains the reservation DAG that forwards reservations from Libre Booking to the Bastion host, by scheduling the user SSH keys based on the created resource reservation in Libre Booking. 

The DAG is developed for Airflow. It consists of two parts that are defined inside the tests folder:

- DAG definition in a Python file that is in the dags folder
- configuration JSON file that holds the information on how to connect to the two systems that is in the config folder

To use the DAG you need to copy its definition in your Aifrlow dags folder and the configuration in your Airflow config folder.
Then edit the configuration to provide correct values for the URLS and access credentials to your Libre Booking and Bastion hosts.

Feel free to modify the DAG to suit to your needs and purposes.


