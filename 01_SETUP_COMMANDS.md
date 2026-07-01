# Spark + Hadoop Setup Guide

Step-by-step commands to install Apache Spark 3.5.1 on an existing Hadoop 3.3.6 / YARN cluster.
Use this alongside the main [README.md](README.md).

## STEP 1: Switch to Hadoop User
```bash
su hadoop
```
Enter the hadoop user's password when prompted.

---

## STEP 2: Download Spark (Without Hadoop)

**Option A: Download with sudo (Recommended)**
```bash
sudo wget -O /opt/spark-3.5.1-bin-without-hadoop.tgz https://archive.apache.org/dist/spark/spark-3.5.1/spark-3.5.1-bin-without-hadoop.tgz
```

**Option B: Download to home first, then move**
```bash
cd ~
wget https://archive.apache.org/dist/spark/spark-3.5.1/spark-3.5.1-bin-without-hadoop.tgz
sudo mv spark-3.5.1-bin-without-hadoop.tgz /opt/
```

Alternative (if wget is slow, use curl):
```bash
sudo curl -O -L https://archive.apache.org/dist/spark/spark-3.5.1/spark-3.5.1-bin-without-hadoop.tgz -o /opt/spark-3.5.1-bin-without-hadoop.tgz
```

---

## STEP 3: Extract Spark
```bash
cd /opt
sudo tar xvzf spark-3.5.1-bin-without-hadoop.tgz
sudo ln -sf /opt/spark-3.5.1-bin-without-hadoop /opt/spark
sudo chown -R hadoop:hadoop /opt/spark-3.5.1-bin-without-hadoop /opt/spark
cd /opt/spark
```

Verify extraction:
```bash
ls -la /opt/spark/
```

You should see: `bin/`, `conf/`, `data/`, `jars/`, `python/`, `R/`, `licenses/`, etc.

---

## STEP 4: Edit .bashrc to Add Environment Variables
```bash
nano ~/.bashrc
```

Add these lines at the END of the file:
```bash
# Hadoop Configuration
export HADOOP_HOME=/home/hadoop/hadoop-3.3.6
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export PATH=$PATH:$HADOOP_HOME/bin

# Spark Configuration
export YARN_CONF_DIR=$HADOOP_HOME/etc/hadoop
export SPARK_HOME=/opt/spark
export SPARK_DIST_CLASSPATH=$(hadoop classpath)
export PATH=$PATH:$SPARK_HOME/bin
export LD_LIBRARY_PATH=$HADOOP_HOME/lib/native:$LD_LIBRARY_PATH

# Python Spark Configuration
export PYSPARK_PYTHON=/usr/bin/python3
```

Save file (Ctrl+O, Enter, Ctrl+X)

Apply changes:
```bash
source ~/.bashrc
```

---

## STEP 5: Configure Spark with Hadoop/YARN
Edit spark-env.sh:
```bash
nano $SPARK_HOME/conf/spark-env.sh
```

Add these lines:
```bash
export SPARK_DIST_CLASSPATH=$(hadoop classpath)
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
```

Save (Ctrl+O, Enter, Ctrl+X)

---

## STEP 6: Configure Spark Master for YARN
Edit spark-defaults.conf:
```bash
nano $SPARK_HOME/conf/spark-defaults.conf
```

Add/uncomment this line:
```bash
spark.master yarn
```

Save (Ctrl+O, Enter, Ctrl+X)

---

## STEP 7: Verify Environment Variables
```bash
echo $HADOOP_HOME
echo $SPARK_HOME
echo $SPARK_DIST_CLASSPATH
```

Should output:
- `/home/hadoop/hadoop-3.3.6`
- `/opt/spark`
- (long classpath with jar files)

---

## STEP 8: Start Hadoop Services
```bash
$HADOOP_HOME/sbin/start-dfs.sh
$HADOOP_HOME/sbin/start-yarn.sh
```

Wait 10 seconds for services to start.

---

## STEP 9: Verify Services Running
```bash
jps
```

Should show:
```
NameNode
DataNode
ResourceManager
NodeManager
Jps
```

---

## STEP 10: Start PySpark Shell
```bash
pyspark
```

Or with YARN:
```bash
pyspark --master yarn
```

---

## TROUBLESHOOTING

**Issue: Command not found (hadoop)**
- Solution: Check `echo $HADOOP_HOME` and ensure it's set

**Issue: PySpark won't start**
- Solution: Install Python3-dev: `sudo apt-get install python3-dev`

**Issue: Java not found**
- Solution: `sudo apt-get install openjdk-11-jdk`

---

## Quick Service Control

**Stop all services:**
```bash
$HADOOP_HOME/sbin/stop-yarn.sh
$HADOOP_HOME/sbin/stop-dfs.sh
```

**Check if services are running:**
```bash
jps
```

**View Spark UI (after running spark code):**
```
http://localhost:4040
```

