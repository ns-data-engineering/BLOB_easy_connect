# coding: utf-8
# -------------------------------------------------------------------------
# Desenvolvido a partir do modelo oficial Microsoft blob for PY
# -------------------------------------------------------------------------
# LIB azure-storage-blob neceaario - use pip install azure-storage-blob

# Importacao de dependencias
from azure.storage.blob import BlockBlobService, ContentSettings, AppendBlobService
import os


def criaContainerAzure(account, key, containerName):
    # Esse metodo verificar cria um container somente apos testar se o mesmo ja existe

    blobService = BlockBlobService(account, key)

    if blobService.exists(containerName):
        return "O container ja existe, impossivel de criar itens duplicados."

    else:

        blobService.create_container(containerName)

        return "Container criado com sucesso."

def deletaContainerAzure(account, key, containerName):
    # Esse metodo deleta um container somente apos testar se o mesmo existe

    blobService = BlockBlobService(account, key)

    if blobService.exists(containerName):

        blobService.delete_container(containerName)

        return "O container foi excluido com sucesso."

    else:

        return "O container nao especificado nao existe."

def criaBlobAzure(account, key, containerName, blobName, file):
    # Esse metodo deleta um container somente apos testar se o mesmo existe

    blobService = BlockBlobService(account, key)

    if blobService.exists(containerName):

        blobService.create_blob_from_path(containerName, blobName, file,
                                              content_settings=ContentSettings(content_type='text/csv'))

        return "Blob " + blobName + " criado com sucesso no container: " + containerName + ". \nConta de criacao: " + account + ""

    else:

        return "O container especificado nao existe. O blob NAO foi criado com sucesso"

def criaBlobApendavelAzure(account, key, containerName, blobName, file):
    # Nao implementado

    return "Nao implementado"

def criaBlobAzure_comPasta(account, key, containerName, blobName, file, folder):
    # Esse metodo deleta um container somente apos testar se o mesmo existe

    blobService = BlockBlobService(account, key)

    if blobService.exists(containerName):

        blobService.create_blob_from_path(containerName, folder+"/"+blobName, file,
                                              content_settings=ContentSettings(content_type='text/csv'))

        return "Blob " + blobName + " criado com sucesso no container: " + containerName + ". \nConta de criacao: " + account + ""

    else:

        return "O container especificado nao existe. O blob NAO foi criado com sucesso"

def criaBlobAzure_multiplosArquivos(account, key, containerName, folder):
    # Esse metodo faz o upload de todos os arquivos em uma pasta

    blobService = BlockBlobService(account, key)

    files = os.listdir(folder)

    if blobService.exists(containerName):

        for name in files:

            blobService.create_blob_from_path(containerName, os.path.basename(name), folder + "\\" + name,
                                              content_settings=ContentSettings(content_type='text/csv'))

            print(folder + "\\" + name + "| Carregado no blob")

        return "Multiplos arquivos criados no blob | Criado no container: " + containerName + ""

    else:

        return "O container especificado nao existe. O blob NAO foi criado com sucesso"

def deletaBlobAzure(account, key, containerName, blobName):
    # Esse metodo deleta um container somente apos testar se o mesmo existe

    blobService = BlockBlobService(account, key)

    if blobService.exists(containerName):

        blobService.delete_blob(containerName, blobName)

        return "Blob: " + blobName + "| Deletado com sucesso"

    else:

        return "O container especificado nao existe. O blob NAO foi criado com sucesso"

def baixaBlobAzure(account, key, containerName, blobName, caminho):
    # Faz o download de um blob specifico de um container expecifico

    blobService = BlockBlobService(account, key)

    if blobService.exists(containerName):

        blobService.get_blob_to_path(containerName, blobName, caminho+"/"+blobName)

        return "Blob: " + blobName + "| Criado com sucesso"

    else:

        return "O container especificado nao existe. O blob nao foi baixado com sucesso"

def listarBlobAzure(account, key, containerName):
    # Lista todos os blobs de um container

    blobService = BlockBlobService(account, key)

    if blobService.exists(containerName):

        generator = blobService.list_blobs(containerName)

        listaBlobs = ""

        for blob in generator:

            print(blob.name)

            listaBlobs = listaBlobs + str(blob.name) + "|"

        print("Lista com todos os blobs nesse container.")

        return listaBlobs

    else:

        blobService.create_container(containerName)

        return "O container citado nao existe."

