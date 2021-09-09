import matplotlib.pyplot as plt
import pyspark
from pyspark import SparkContext
from pyspark.sql import SparkSession, SQLContext, HiveContext
from pyspark.sql.functions import *
from fpdf import FPDF
from PIL import Image
import glob
import matplotlib.cm as cm
import numpy as np

import os
import pymongo
import pandas as pd
import json
from helper_funcs import *
from client_class import Client

cursor_y = 0

WIDTH = 210
HEIGHT = 297

class PDF(FPDF):
    def header(self):
        self.set_font('Arial', 'B', 16)
        if self.page_no() == 1:
            self.image("./letterheadgreen.png",0,0,210)
            self.image("./ibmlogo.png", 170, 20, 25)
        else:
            self.image("./ibmlogo.png", 183, 20, 15)
        self.set_line_width(1)
        self.ln(10)

    def add_small_title(self, text):
        global cursor_y

        if cursor_y+100>210:
            cursor_y = 30
            self.add_page()
        
        self.set_y(cursor_y)
        self.set_font("Arial", size=10)
        self.ln(10)
        self.cell(w=0, h=6, txt=text, ln=4, align='L')

        cursor_y+=20

    def add_titre(self, name, day):
        global cursor_y
        self.set_font("Arial", size = 24)
        self.ln(25)
        self.cell(w=0, txt = name, ln = 5, align = 'L')
        self.set_font("Arial", size = 15)
        self.ln(10)
        self.cell(w=0, txt = f'{day}', ln = 4, align = 'L')
        self.ln(8)
        self.set_font("Arial", size = 12)
        self.cell(w=0, txt = "Percentage CPU usage of every client's hosts", ln = 5, align = 'L')


        cursor_y = 67.75
        # 24pt + 35mm + 5mm + 15pt + 10mm + 4mm =
        # 39pt + 54mm
        # 67.7584 from top

    def footer(self):
        self.image("./footergreen.png", -7, 283, WIDTH)
        self.set_y(-8)
        self.set_font("Arial", size = 16)
        self.cell(w= 0, h=0, txt = str(self.page_no()) + '        ', ln = 3, align = 'R')

    def add_two_figures(self, figure1, figure2):
        global cursor_y
        # assuming fig sizes are 640x480
        fig_w = 640
        fig_h = 480

        new_w = 100 # width/2 -5
        new_h = (fig_h/fig_w)*new_w

        if cursor_y + new_h > HEIGHT:
            self.add_page()
            cursor_y = 30

        self.image(figure1, 5, cursor_y, new_w)
        self.image(figure2, 5+new_w, cursor_y, new_w)

        cursor_y += new_h

        if cursor_y > 290: # if out of bounds
            cursor_y = 30
            self.add_page()
    
    def add_single_figure(self, figure):
        global cursor_y
        # assuming fig size for single figure is 1000x350
        fig_w = 640
        fig_h = 480
        new_w = 100 # WIDTH
        new_h = (fig_h/fig_w)*new_w
        if cursor_y + new_h > HEIGHT:
            self.add_page()
            cursor_y = 30
        self.image(figure, 55, cursor_y, new_w)
        cursor_y += new_h
        if cursor_y > 240:
            cursor_y = 30
            self.add_page()

    def print_chapter(self, num, title, name):
        self.add_page()
        self.chapter_title(num, title)
        self.chapter_body(name)

    def add_info(self, text):
        global cursor_y
        self.set_y(cursor_y)
        self.set_font("Arial", size = 11)
        self.ln(5)
        self.cell(w=0, txt = text, ln = 2, align = 'L')
        # 15pt + 14 mm
        # 5.2917 + 14
        # 19.2917
        cursor_y += 4
        if cursor_y > 290:
            cursor_y = 30
            self.add_page()

class ReportForIBM:
    def __init__(self, dataframe, client_dfs, client_names, spark):
        self.client_objects = {}
        self.client_names = client_names
        self.dataframe = dataframe #all dataframe

        self.cpu_array = []

        self.mem_array = []

        self.disk_array = []

        for p in os.listdir("./ibmcharts/"):
            if p.endswith("allhosts.jpg"):
                self.cpu_array.append(p)


        for p in os.listdir("./memcharts/"):
            if p.endswith("allhosts.jpg"):
                self.mem_array.append(p)


        for p in os.listdir("./diskcharts/"):
            if p.endswith("allhosts.jpg"):
                self.disk_array.append(p)

        self.cpu_array = ["./ibmcharts/"+i for i in self.cpu_array]
        self.mem_array = ["./memcharts/"+i for i in self.mem_array]
        self.disk_array = ["./diskcharts/"+i for i in self.disk_array]
        print(self.cpu_array)
        print(self.mem_array)
        print(self.disk_array)

        # self.path_array = []
        # self.path_array2 = []

        # for client in client_names:
        #     self.client_objects[client] = Client(client_dataframes[client], spark, client)

        self.print_charts()

    def client_hosts_graphs(self, pdf):
        for client in self.client_names:
            self.path_array.append(self.all_hosts_pie_chart(pdf, self.client_objects[client]))
            self.path_array2.append(self.all_hosts_mem_chart(pdf, self.client_objects[client]))

    def print_charts(self):
        pdf = PDF()
        pdf.add_page()
        pdf.add_titre("Overall Report", "07/09/2021")
        #self.client_usage()
        #self.client_hosts_graphs(pdf)

        if(len(self.cpu_array)%2==0):
            for i in range(0, len(self.cpu_array)):
                pdf.add_two_figures(self.cpu_array[i], self.cpu_array[i+1])
                i += 2
        else:
            for i in range(0, len(self.cpu_array)-1, 2):
                pdf.add_two_figures(self.cpu_array[i], self.cpu_array[i+1])
            pdf.add_single_figure(self.cpu_array[len(self.cpu_array)-1])
        pdf.ln(8)
        pdf.set_font("Arial", size = 12)
        pdf.cell(w=0, txt = "Percentage CPU usage of every client", ln = 5, align = 'L')
        pdf.add_single_figure("./ibmcharts/zorallhosts.jpg")

        pdf.add_small_title("Percentage CPU usage of every client's hosts")
        #add Memory Data
        if(len(self.mem_array)%2==0):
            for i in range(0, len(self.mem_array)):
                pdf.add_two_figures(self.mem_array[i], self.mem_array[i+1])
                i += 2
        else:
            for i in range(0, len(self.mem_array)-1, 2):
                pdf.add_two_figures(self.mem_array[i], self.mem_array[i+1])
            pdf.add_single_figure(self.mem_array[len(self.mem_array)-1])


        pdf.output("ibmreports/ibm.pdf")

    def client_usage(self):
        client_hours = []
        cli = []
        client_dataframes = []

        for i in self.client_names:
            client_dataframes.append(self.dataframe.filter(self.dataframe.Server_Name.contains(i) & self.dataframe.Processor.contains('_Total')))

        for client in client_dataframes:
            df = client.select('AVG_%_Processor_Time').groupBy().sum()
            client_hours.append(df.toPandas()["sum(AVG_%_Processor_Time)"].values.tolist())

        for c in client_hours:
            cli.append(c[0])

        sizes = cli
        labels = ['aig', 'eti', 'tuv', 'aho', 'zor']

        colors = cm.rainbow(np.linspace(0, 1, len(sizes)))
        plt.gca().axis("equal")
        plt.pie(sizes, labels=labels, colors=colors, autopct = '%1.1f%%', pctdistance=1.25, labeldistance=0.9, textprops={'fontsize': 8})
        plt.title("Total Percentage Usage per Host in 1 month")
        path = "./ibmcharts/allclients.jpg"
        plt.savefig(path)
        width = WIDTH/2
        return path, width

    def all_hosts_pie_chart(self, pdf, client):
        plt.clf()
        labels = client.hostnames
        sizes = []
        d2_sizes = []
        for host in client.hostnames:
            df = client.hosts_dataframes[host].select('AVG_%_Processor_Time').groupBy().sum()
            d2_sizes.append(df.toPandas()["sum(AVG_%_Processor_Time)"].values.tolist())
        
        for i in range(0,len(d2_sizes)):
            sizes.append(d2_sizes[i][0])

        colors = cm.rainbow(np.linspace(0, 1, len(sizes)))
        plt.gca().axis("equal")
        plt.pie(sizes, labels=labels, colors=colors, autopct = '%1.1f%%', pctdistance=1.25, labeldistance=0.9, textprops={'fontsize': 8})
        plt.title("Percentage CPU Usage in 1 month for "+client.client_name)
        path = "./ibmcharts/"+client.client_name+'allhosts.jpg'
        plt.savefig(path)
        width = 3*WIDTH/5
        return path

    def all_hosts_mem_chart(self, pdf, client):
        plt.clf()
        labels = client.hostnames
        sizes = []
        d2_sizes = []
        for host in client.hostnames:
            df = client.hosts_dataframes[host].select('AVG_Memory_Usage_Percentage').groupBy().sum()
            d2_sizes.append(df.toPandas()["sum(AVG_Memory_Usage_Percentage)"].values.tolist())
        
        for i in range(0,len(d2_sizes)):
            sizes.append(d2_sizes[i][0])

        colors = cm.rainbow(np.linspace(0, 1, len(sizes)))
        plt.gca().axis("equal")
        plt.pie(sizes, labels=labels, colors=colors, autopct = '%1.1f%%', pctdistance=1.25, labeldistance=0.9, textprops={'fontsize': 8})
        plt.title("Percentage Memory Usage for" + self.client.client_name + "in 1 month")
        path = "./memcharts/"+client.client_name+'allhosts.jpg'
        plt.savefig(path)
        width = 3*WIDTH/5
        return path, width


if __name__ == "__main__":
    conf = pyspark.SparkConf().set("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1").setMaster("local").setAppName("newApp").setAll([("spark.driver.memory", "15g"), ("spark.executer.memory", "20g")])
    sc = SparkContext(conf=conf)

    sqlC = SQLContext(sc)

    spark = SparkSession.builder \
    .master('local[*]') \
    .config("spark.driver.memory", "15g") \
    .appName('newApp') \
    .getOrCreate()

    # #load_to_database("/home/basan/internship-codes/busra_cpu.xlsx", "basanto")
    load_to_database("./data/cpu.xlsx", "basanto")
    print("hello")
    mongo_ip = "mongodb://localhost:27017/basanto."
    iris = read_from_database(mongo_ip, sqlC)
    dataframe = create_partition(iris, spark)

    clients = [] #names of clients
    client_dataframes = {} #dataframes of clients
    clients, client_dataframes = extract_dataframes(dataframe)
    client_objects = {} #dictionary of client objects
    
    ReportForIBM(dataframe, client_dataframes, clients, spark)
