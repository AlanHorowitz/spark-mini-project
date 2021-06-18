## Spark Mini Project

### Perform spark transformations in Hortonworks sandbox

### Demonstration instructions

- Install HortonWorks HDP Sandbox 3.0 as a VirtualBox appliance
- Create admin user and /user/admin/input folder
- Update admin and input folders to full write permissions
- Log into sandbox terminal as root
- git clone https://github.com/AlanHorowitz/spark-mini-project.git
- hadoop fs -put data.csv /user/admin/input
- spark-submit main.py > results.txt >2&1

### Results

- Output writen to /user/admin/output, copies to local output older
- Output log at results.txt
