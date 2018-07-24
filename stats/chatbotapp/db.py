#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Oct  6 10:51:27 2017

@author: rahul
"""
import configparser
import logging
import os
import sys

from pymongo import MongoClient


class databaseOperations:
    '''
    class databaseOperations
    '''

    def __init__(self):
        '''
        constructor
        :param self
        access all parameter to connect mongodb  from setting.ini file
        '''
        config = configparser.ConfigParser()
        config.read('/home/mv/PycharmProjects/chat/chatbot/chatbotapp/settings.ini')
        self.user = config.get('mongo', 'username')
        self.port = int(config.get('mongo', 'port'))
        self.dbname = config.get('mongo', 'dbname')
        self.password = config.get('mongo', 'password')
        self.hostname = config.get('mongo', 'hostname')
        self.client = None
        self.logger = logging.getLogger(__name__)
        self.connect_mongodb()

        # connect creation to mongodb

    def connect_mongodb(self):
        try:
            self.client = MongoClient(self.hostname, self.port)
            self.client.admin.authenticate(self.user, self.password, mechanism='SCRAM-SHA-1', source=self.dbname)
            self.logger.info("Connected to database server....")
        except Exception as e:
            self.logger.info("Could not establish connection to database server....")
            self.logger.error(str(e))
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname1 = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            self.logger.error(exc_type, fname1, exc_tb.tb_lineno)


    def find_meters_all(self, collection_name):

        '''
        find document using a key from give collection
        :param collection_name:
        :param key:
        :return:none

        '''

        try:
            db = self.client[self.dbname]
            collection = db[str(collection_name)]
            data = collection.find({})
            #            for i in data:
            #
            #                print(i)
            return data
        except Exception as e:
            self.logger.info("Could not find the document or record ")
            self.logger.error(str(e))
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname1 = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            self.logger.error(exc_type, fname1, exc_tb.tb_lineno)
            return None


    def find_meter(self, collection_name, meter_id):

        '''
        find document using a key from give collection
        :param collection_name:
        :param key:
        :return:none

        '''

        try:
            db = self.client[self.dbname]
            collection = db[str(collection_name)]
            data = collection.find({"medidor_id": meter_id})
            #            for i in data:
            #
            #                print(i)
            return data
        except Exception as e:
            self.logger.info("Could not find the document or record ")
            self.logger.error(str(e))
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname1 = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            self.logger.error(exc_type, fname1, exc_tb.tb_lineno)
            return None

    # def find_pdf_record(self, collection_name, filename):
    #
    #     '''
    #     find document using a key from give collection
    #     :param collection_name:
    #     :param key:
    #     :return:none
    #
    #     '''
    #
    #     try:
    #         db = self.client[self.dbname]
    #         collection = db[str(collection_name)]
    #         filename = str(filename)
    #         collection.find({"filename": filename})
    #         #            for i in data:
    #         #
    #         #                print(i)
    #         return True
    #
    #     except Exception as e:
    #         self.logger.info("Could not find the document or record ")
    #         self.logger.error(str(e))
    #         exc_type, exc_obj, exc_tb = sys.exc_info()
    #         fname1 = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
    #         self.logger.error(exc_type, fname1, exc_tb.tb_lineno)
    #         return False
    #
    # def find_record_in_doctable(self, collection_name, status):
    #
    #     '''
    #     find document using a key from give collection
    #     :param collection_name:
    #     :param key:
    #     :return:none
    #
    #     '''
    #
    #     try:
    #         db = self.client[self.dbname]
    #         collection = db[str(collection_name)]
    #         data = collection.find({"status": status})
    #         #            for i in data:
    #         #
    #         #                print(i)
    #         return data
    #     except Exception as e:
    #         self.logger.info("Could not find the document or record ")
    #         self.logger.error(str(e))
    #         exc_type, exc_obj, exc_tb = sys.exc_info()
    #         fname1 = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
    #         self.logger.error(exc_type, fname1, exc_tb.tb_lineno)
    #         return None
    #
    # def find_record_in_doctable_or_case(self, collection_name, status1, status2):
    #
    #     '''
    #     find document using a key from give collection
    #     :param collection_name:
    #     :param key:
    #     :return:none
    #
    #     '''
    #
    #     try:
    #         db = self.client[self.dbname]
    #         collection = db[str(collection_name)]
    #         data = collection.find({"status": {"$in": [status1, status2]}})
    #         #            for i in data:
    #         #
    #         #                print(i)
    #         return data
    #     except Exception as e:
    #         self.logger.info("Could not find the document or record ")
    #         self.logger.error(str(e))
    #         exc_type, exc_obj, exc_tb = sys.exc_info()
    #         fname1 = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
    #         self.logger.error(exc_type, fname1, exc_tb.tb_lineno)
    #         return None
    #
    # def find_application(self, collection_name, appno):
    #
    #     '''
    #     find document using a key from give collection
    #     :param collection_name:
    #     :param key:
    #     :return:none
    #
    #     '''
    #
    #     try:
    #         db = self.client[self.dbname]
    #         collection = db[str(collection_name)]
    #         data = collection.find({"appno": appno})
    #         #            for i in data:
    #         #
    #         #                print(i)
    #         return data
    #     except Exception as e:
    #         self.logger.info("Could not find the document or record ")
    #         self.logger.error(str(e))
    #         exc_type, exc_obj, exc_tb = sys.exc_info()
    #         fname1 = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
    #         self.logger.error(exc_type, fname1, exc_tb.tb_lineno)
    #         return None
    #
    # def find_application_uid(self, collection_name, uid):
    #
    #     '''
    #     find document using a key from give collection
    #     :param collection_name:
    #     :param key:
    #     :return:none
    #
    #     '''
    #
    #     try:
    #         db = self.client[self.dbname]
    #         collection = db[str(collection_name)]
    #         data = collection.find({"uid": uid})
    #         #            for i in data:
    #         #
    #         #                print(i)
    #         return data
    #     except Exception as e:
    #         self.logger.info("Could not find the document or record ")
    #         self.logger.error(str(e))
    #         exc_type, exc_obj, exc_tb = sys.exc_info()
    #         fname1 = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
    #         self.logger.error(exc_type, fname1, exc_tb.tb_lineno)
    #         return None
    #
    # def insert_into_collection(self, collection_name, AppNo, appdata, intime, acc_status_code, gen_excel_status,
    #                            no_of_docs, authkey):
    #     '''
    #     insertion of document
    #     :param collection_name:
    #     :param key:
    #     :param data:
    #     :return: none
    #     '''
    #     #
    #
    #     try:
    #         db = self.client[self.dbname]
    #         collection = db[str(collection_name)]
    #
    #         collection.insert_one(
    #             {"AppNo": AppNo, "appdata": appdata, "intime": intime, "acc_status_code": acc_status_code,
    #              "gen_excel_status": gen_excel_status, "no_of_docs": no_of_docs, "authkey": authkey})
    #         self.logger.info("Record inserted")
    #     except Exception as e:
    #         self.logger.info("Record insertion failed")
    #         self.logger.error(str(e))
    #         exc_type, exc_obj, exc_tb = sys.exc_info()
    #         fname1 = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
    #         self.logger.error(exc_type, fname1, exc_tb.tb_lineno)
    #
    # def insert_into_collection_tab2(self, collection_name, uid, pid, wid, Appno, Sname, intime, stime, dur, status,
    #                                 bankname, language, authkey, noofpages, message, drymode, accuracy,
    #                                 no_of_corrections, editaccstatus, editurl, luigi_status_code, doc_pull_intime,
    #                                 doc_pull_stime):
    #     '''
    #     insertion of document
    #     :param collection_name:
    #     :param key:
    #     :param data:
    #     :return: none
    #     '''
    #     #
    #
    #     try:
    #         db = self.client[self.dbname]
    #         collection = db[str(collection_name)]
    #
    #         collection.insert_one(
    #             {"uid": uid, "pid": pid, "wid": wid, "appno": Appno, "sname": Sname, "intime": intime, "stime": stime,
    #              "dur": dur, "status": status, "bankname": bankname, "language": language, "authkey": authkey,
    #              "no_of_pages": noofpages, "message": message, "drymode": drymode, "accuracy": accuracy,
    #              "no_of_corrections": no_of_corrections, "edit_acc_status": editaccstatus, "edit_url": editurl,
    #              "luigi_status_code": luigi_status_code, "doc_pull_intime": doc_pull_intime,
    #              "doc_pull_stime": doc_pull_stime})
    #         self.logger.info("Record inserted")
    #     except Exception as e:
    #         self.logger.info("Record insertion failed")
    #         self.logger.error(str(e))
    #         exc_type, exc_obj, exc_tb = sys.exc_info()
    #         fname1 = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
    #         self.logger.error(exc_type, fname1, exc_tb.tb_lineno)
    #
    # def update_collection(self, collection_name, key, value, job_pull_intime, job_pull_stime):
    #     '''
    #     update collections's document using key but key
    #     :param collection_name:
    #     :param key:
    #     :param value:
    #     :return: none
    #     '''
    #
    #     try:
    #         db = self.client[self.dbname]
    #         collection = db[str(collection_name)]
    #         collection.update_one({"appno": key}, {
    #             "$set": {"acc_status_code": value, "job_pull_intime": job_pull_intime,
    #                      "job_pull_stime": job_pull_stime}})
    #         self.logger.info("records are updated")
    #     except Exception as e:
    #         self.logger.info("Records are not updated")
    #         self.logger.error(str(e))
    #         exc_type, exc_obj, exc_tb = sys.exc_info()
    #         fname1 = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
    #         self.logger.error(exc_type, fname1, exc_tb.tb_lineno)
    #
    # def update_collection_doctable_status_sname(self, collection_name, uid_key, status, sname):
    #     '''
    #     update collections's document using key but key
    #     :param collection_name:
    #     :param key:
    #     :param value:
    #     :return: none
    #     '''
    #
    #     try:
    #         db = self.client[self.dbname]
    #         collection = db[str(collection_name)]
    #         collection.update_one({"uid": uid_key}, {"$set": {"status": status, "sname": sname}})
    #         self.logger.info("records are updated")
    #     except Exception as e:
    #         self.logger.info("Records are not updated")
    #         self.logger.error(str(e))
    #         exc_type, exc_obj, exc_tb = sys.exc_info()
    #         fname1 = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
    #         self.logger.error(exc_type, fname1, exc_tb.tb_lineno)
    #
    # def update_collection_doctable_pid_status_acc_noofcorr_msg_editacc_editurl_sname(self, collection_name, uid_key,
    #                                                                                  pid, status, accuracy,
    #                                                                                  no_of_corrections, message,
    #                                                                                  editaccstatus, editurl, sname):
    #     '''
    #     update collections's document using key but key
    #     :param collection_name:
    #     :param key:
    #     :param value:
    #     :return: none
    #     '''
    #
    #     try:
    #         db = self.client[self.dbname]
    #         collection = db[str(collection_name)]
    #         collection.update_one({"uid": uid_key}, {
    #             "$set": {"pid": pid, "status": status, "accuracy": accuracy, "no_of_corrections": no_of_corrections,
    #                      "message": message, "edit_acc_status": editaccstatus, "edit_url": editurl, "sname": sname}})
    #         self.logger.info("records are updated")
    #     except Exception as e:
    #         self.logger.info("Records are not updated")
    #         self.logger.error(str(e))
    #         exc_type, exc_obj, exc_tb = sys.exc_info()
    #         fname1 = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
    #         self.logger.error(exc_type, fname1, exc_tb.tb_lineno)
    #
    # def update_collection_doctable_based_on_ocr_response(self, collection_name, uid_key, message, statusCode,
    #                                                      no_of_pages, sname):
    #     '''
    #     update collections's document using key but key
    #     :param collection_name:
    #     :param key:
    #     :param value:
    #     :return: none
    #     '''
    #
    #     try:
    #         db = self.client[self.dbname]
    #         collection = db[str(collection_name)]
    #         collection.update_one({"uid": uid_key}, {
    #             "$set": {"message": message, "status": statusCode, "no_of_pages": no_of_pages, "sname": sname}})
    #         self.logger.info("records are updated")
    #     except Exception as e:
    #         self.logger.info("Records are not updated")
    #         self.logger.error(str(e))
    #         exc_type, exc_obj, exc_tb = sys.exc_info()
    #         fname1 = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
    #         self.logger.error(exc_type, fname1, exc_tb.tb_lineno)
    #
    # def update_collection_doctables_doc_pull_in_and_stime(self, collection_name, uid_key, doc_pull_intime,
    #                                                       doc_pull_stime):
    #     '''
    #     update collections's document using key but key
    #     :param collection_name:
    #     :param key:
    #     :param value:
    #     :return: none
    #     '''
    #
    #     try:
    #         db = self.client[self.dbname]
    #         collection = db[str(collection_name)]
    #         collection.update_one({"uid": uid_key},
    #                               {"$set": {"doc_pull_intime": doc_pull_intime, "doc_pull_stime": doc_pull_stime}})
    #         self.logger.info("records are updated")
    #     except Exception as e:
    #         self.logger.info("Records are not updated")
    #         self.logger.error(str(e))
    #         exc_type, exc_obj, exc_tb = sys.exc_info()
    #         fname1 = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
    #         self.logger.error(exc_type, fname1, exc_tb.tb_lineno)
    #
    # def sort_collection(self, collection_name, acc_status_code):
    #
    #     try:
    #         db = self.client[self.dbname]
    #         collection = db[str(collection_name)]
    #         data = collection.find({"acc_status_code": acc_status_code}).sort(
    #             [("intime", 1)])  # intime ascending and status decending
    #         return data
    #     except Exception as e:
    #         self.logger.error(str(e))
    #         exc_type, exc_obj, exc_tb = sys.exc_info()
    #         fname1 = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
    #         self.logger.error(exc_type, fname1, exc_tb.tb_lineno)
    #         return None
    #
    # def sort_collection_doc_table(self, collection_name, status):
    #
    #     try:
    #         db = self.client[self.dbname]
    #         collection = db[str(collection_name)]
    #         data = collection.find({"status": status}).sort([("intime", 1)])  # intime ascending and status decending
    #         return data
    #     except Exception as e:
    #         self.logger.error(str(e))
    #         exc_type, exc_obj, exc_tb = sys.exc_info()
    #         fname1 = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
    #         self.logger.error(exc_type, fname1, exc_tb.tb_lineno)
    #         return None
    #
    # def remove_document(self, collection_name, key):
    #     '''
    #     remove document from collection given a key
    #     :param collection_name:
    #     :param key:
    #     :return:
    #     '''
    #
    #     try:
    #         db = self.client[self.dbname]
    #         collection = db[str(collection_name)]
    #         collection.remove_one({"uid": key})
    #         self.logger.info("selected document are removed")
    #     except Exception as e:
    #         self.logger.info("Document could not be deleted")
    #         self.logger.error(str(e))
    #         exc_type, exc_obj, exc_tb = sys.exc_info()
    #         fname1 = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
    #         self.logger.error(exc_type, fname1, exc_tb.tb_lineno)


# db_obj = databaseOperations()
# from bson.json_util import dumps
# import ast
# x = db_obj.find_meter('meters',2077688)
# appdata = dumps(x)
# appdata = ast.literal_eval(appdata)