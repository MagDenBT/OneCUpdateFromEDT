import configparser
import logging
import os
import subprocess
import datetime
from threading import Thread


class DeltaTimeFormatter(logging.Formatter):

    def format(self, record):
        duration = datetime.datetime.utcfromtimestamp(record.relativeCreated / 1000)
        record.delta = duration.strftime("%H:%M:%S")
        return super().format(record)


class Args(object):
    # Paths settings
    """
            The directory structure is as follows:
            On the local machine - disk:\rootDir\customDir\fullBpDir and for WAL-files - the one you specify
            In the cloud - /rootDir/customDir/fullBpDir and
                    for WAL-files - /rootDir/customDir/"LastDirectoryIn(localPathToWALFiles)"
            """
    __ring: str = None
    __ring_output_path: str = None
    __edt_workspace_location: str = None
    __edt_project: str = None
    __edt_module: str = None
    __ibcmd: str = None
    __db_server: str = None
    __db_name: str = None
    __db_user: str = None
    __db_pwd: str = None
    __user_1c: str = None
    __pass_1c: str = None
    __ibcmd_data: str = None

    def __getitem__(self, item):
        return getattr(self, item)

    def __init__(self, args=None):
        self.set_params(args)

    def set_params(self, args=None):
        if args is not None:
            for key, value in args.items():
                set_method = self[f'set_{key}']
                set_method(value)
        else:
            self.__set_default_params()

    def __set_default_params(self):
        self.__ring = 'C:/Program Files/1C/1CE/components/1c-enterprise-ring-0.11.10+2-x86_64/ring.cmd'
        self.__ring_output_path = 'C:/jenkinsSlave/CF_XML'
        self.__edt_workspace_location = 'C:/EDT projects/Тест гита'
        self.__edt_project = 'C:/Users/magde/git/proj_2_3_11_375/proj_2_3_11_375'
        self.__edt_module = 'edt@2022.1.4:x86_64'
        self.__ibcmd = 'C:/Program Files/1cv8/8.3.20.2076/bin/ibcmd.exe'
        self.__db_server = 'localhost'
        self.__db_name = 'Base1c'
        self.__db_user = 'sa'
        self.__db_pwd = '22222'
        self.__user_1c = '1cUser'
        self.__pass_1c = '1111'
        self.__ibcmd_data = 'C:/jenkinsSlave/DB_UPDATE'

    # Getters

    def ring(self):
        return self.__ring

    def ring_output_path(self):
        return self.__ring_output_path

    def edt_workspace_location(self):
        return self.__edt_workspace_location

    def edt_project(self):
        return self.__edt_project

    def edt_module(self):
        return self.__edt_module

    def ibcmd(self):
        return self.__ibcmd

    def db_server(self):
        return self.__db_server

    def db_name(self):
        return self.__db_name

    def db_user(self):
        return self.__db_user

    def db_pwd(self):
        return self.__db_pwd

    def user_1c(self):
        return self.__user_1c

    def pass_1c(self):
        return self.__pass_1c

    def ibcmd_data(self):
        return self.__ibcmd_data

    # Setters

    def set_ring(self, val: str):
        self.__ring = val

    def set_ring_output_path(self, val: str):
        self.__ring_output_path = val

    def set_edt_workspace_location(self, val: str):
        self.__edt_workspace_location = val

    def set_edt_project(self, val: str):
        self.__edt_project = val

    def set_edt_module(self, val: str):
        self.__edt_module = val

    def set_ibcmd(self, val: str):
        self.__ibcmd = val

    def set_db_server(self, val: str):
        self.__db_server = val

    def set_db_name(self, val: str):
        self.__db_name = val

    def set_db_user(self, val: str):

        self.__db_user = val

    def set_db_pwd(self, val: str):

        self.__db_pwd = val

    def set_user_1c(self, val: str):

        self.__user_1c = val

    def set_pass_1c(self, val: str):
        self.__pass_1c = val

    def set_ibcmd_data(self, val: str):
        self.__ibcmd_data = val


class Worker:
    args = None

    def __init__(self, args):
        self.args = args
        os.system('chcp 1251>nul')

    def _execute(self):
        logger = logging.getLogger()
        logger.info("-----------------------------------------------------------------------------------------")
        logger.warning("НАЧАЛО")
        try:
            self._delete_ring_outputs()
            logger.info("Выгрузка в xml. Старт")
            self._export_edt_xml()
            logger.info("Выгрузка в xml. Завершено")

            logger.info("Импорт конфигурации в 1с. Старт")
            self._import_to_one_c()
            logger.info("Импорт конфигурации в 1с. Завершено")

            thread = Thread(target=self._delete_ring_outputs)
            thread.start()
            logger.info("Обновление конфигурации в 1с. Старт")
            self._update_one_c()
            logger.info("Обновление конфигурации в 1с. Завершено")
        except Exception as e:
            logger.error(str(e))
        logger.warning("КОНЕЦ")

    def _delete_ring_outputs(self):
        path = self.args.ring_output_path()
        command = f'rd "{path}" /S /Q'
        process = subprocess.Popen(command, shell=True, stderr=subprocess.PIPE)
        text_error = process.stderr.read().decode()

    def _copy_dir(self, source, destination):
        command = f'robocopy "{source}" "{destination}" /MIR /MT:32 /njh /njs /ndl /nc /ns /NFL /NP'
        process = subprocess.Popen(command, stderr=subprocess.PIPE)
        text_error = process.stderr.read().decode()
        if not text_error == '':
            raise Exception(text_error)

    def _export_edt_xml(self):
        prev_location = self.args.edt_workspace_location()
        workspace_location = prev_location + '_TEMPCOPY'
        self._copy_dir(prev_location, workspace_location)

        process = subprocess.run(
            [self.args.ring(), ' ', self.args.edt_module(),
             'workspace', 'export',
             '--project', f'{self.args.edt_project()}',
             '--configuration-files', f'{self.args.ring_output_path()}',
             '--workspace-location', f'{workspace_location}',
             ],
            stdout=subprocess.PIPE
        )
        text_error = process.stdout.decode('cp1251')

        if ' is not empty.' in text_error:
            raise Exception('Каталог с XML файлами должен быть пуст! Очистите его' + text_error)
        elif 'Workspace is already locked' in text_error:
            raise Exception("EDT workspace заблокирован. Невозможно выгрузить XML")
        elif '[ERROR]' in text_error:
            raise Exception(text_error)


    def _import_to_one_c(self):
        sub_command = 'infobase config import'

        ex_params = f'--dbms MSSQLServer --db-server "{self.args.db_server()}" --db-name "{self.args.db_name()}" --db-user "{self.args.db_user()}" --db-pwd "{self.args.db_pwd()}" -u "{self.args.user_1c()}" -P "{self.args.pass_1c()}" "{self.args.ring_output_path()}"'
        command = f'"{self.args.ibcmd()}" {sub_command} {ex_params}'
        process = subprocess.Popen(command, shell=True, stderr=subprocess.PIPE)
        text_error = process.stderr.read().decode()
        if '[ERROR]' in text_error:
            raise Exception(text_error)

    def _update_one_c(self):

        sub_command = 'infobase config apply'

        ex_params = f'--dbms MSSQLServer --db-server "{self.args.db_server()}" --db-name "{self.args.db_name()}" --db-user "{self.args.db_user()}" --db-pwd "{self.args.db_pwd()}" -u "{self.args.user_1c()}" -P "{self.args.pass_1c()}" "{self.args.ring_output_path()}" --force'
        command = f'"{self.args.ibcmd()}" {sub_command} {ex_params}'
        process = subprocess.Popen(command, shell=True, stderr=subprocess.PIPE)
        text_error = process.stderr.read().decode()
        if '[ERROR]' in text_error:
            raise Exception(text_error)


class Manager:
    __worker = None
    __args = None

    def __init__(self):
        con = self._get_configs()
        self.__args = Args(con)

        logging.basicConfig(level=logging.INFO, filemode="a")
        logger = logging.getLogger()
        fh = logging.FileHandler("1cUpdater.log")

        LOGFORMAT = '+%(delta)s - %(asctime)s - %(levelname)-9s: %(message)s'
        fmt = DeltaTimeFormatter(LOGFORMAT)
        fh.setFormatter(fmt)
        logger.addHandler(fh)
        self.__worker = Worker(self.__args)

    def do_work(self):
        self.__worker._execute()

    def _get_configs(self):
        config = configparser.ConfigParser()
        config.sections()
        l = config.read('configsUpdater.ini', encoding='UTF-8')
        if len(l) == 0:
            l = config.read('configsUpdater_SAMPLE.ini', encoding='UTF-8')
        con = None if len(l) == 0 else config['DEFAULT']
        return con


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    m = Manager()
    m.do_work()
