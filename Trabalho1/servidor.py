import math
import os
import socket
import sys
import threading
import zlib

import protocolo

sys.stdout.reconfigure(encoding='utf-8')

DIRETORIO_BASE = os.path.abspath(os.path.join(os.path.dirname(__file__), 'arquivos_servidor'))

TIMEOUT_SEGMENTO = 0.5
MAX_TENTATIVAS = 10


def nome_valido(nome):
    if not nome:
        return False
    if '/' in nome or '\\' in nome:
        return False
    if ':' in nome:
        return False
    if '..' in nome:
        return False
    if os.path.isabs(nome):
        return False
    return True


def resolver_caminho(nome):
    caminho = os.path.realpath(os.path.join(DIRETORIO_BASE, nome))
    if os.path.commonpath([caminho, DIRETORIO_BASE]) != DIRETORIO_BASE:
        return None
    return caminho


def validar_req(nome):
    if not nome_valido(nome):
        return None, 'nome de arquivo inválido'

    caminho = resolver_caminho(nome)
    if caminho is None:
        return None, 'nome de arquivo inválido'

    if not os.path.isfile(caminho):
        return None, 'arquivo não encontrado'

    return caminho, None


def enviar_segmentos(socket_dedicado, caminho, endereco, num_segmentos):
    try:
        with open(caminho, 'rb') as arquivo:
            for seq in range(num_segmentos):
                dados = arquivo.read(protocolo.TAMANHO_DADOS)
                checksum = zlib.crc32(dados) & 0xFFFFFFFF
                pacote = protocolo.empacotar_dado(seq, checksum, dados)

                tentativas = 0
                confirmado = False
                reenviar = True
                while not confirmado:
                    if reenviar:
                        socket_dedicado.sendto(pacote, endereco)
                        reenviar = False
                    socket_dedicado.settimeout(TIMEOUT_SEGMENTO)
                    try:
                        resposta, origem = socket_dedicado.recvfrom(65536)
                    except socket.timeout:
                        tentativas += 1
                        if tentativas > MAX_TENTATIVAS:
                            socket_dedicado.sendto(
                                protocolo.empacotar_erro('cliente não respondeu, transferência abortada'),
                                endereco,
                            )
                            return
                        print('\n[TIMEOUT] segmento {} não confirmado, reenviando'.format(seq))
                        reenviar = True
                        continue

                    try:
                        tipo, valor = protocolo.desempacotar(resposta)
                    except Exception:
                        continue

                    if tipo == protocolo.ACK and valor == seq:
                        confirmado = True
                        tentativas = 0
                    elif tipo == protocolo.RETR and valor == seq:
                        print('\n[RETR] pedido de retransmissão recebido para o segmento {}, reenviando'.format(seq))
                        reenviar = True
    finally:
        socket_dedicado.close()


def transferir_arquivo(servidor, caminho, endereco):
    tamanho = os.path.getsize(caminho)
    num_segmentos = max(1, math.ceil(tamanho / protocolo.TAMANHO_DADOS))

    socket_dedicado = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        socket_dedicado.bind(('', 0))
        porta_dados = socket_dedicado.getsockname()[1]
        servidor.sendto(protocolo.empacotar_meta(tamanho, num_segmentos, porta_dados), endereco)
        print('\ntransferência para {} atribuída à porta {}'.format(endereco, porta_dados))
    except Exception:
        socket_dedicado.close()
        raise

    thread = threading.Thread(
        target=enviar_segmentos,
        args=(socket_dedicado, caminho, endereco, num_segmentos),
        daemon=True,
    )
    thread.start()


def main():
    if len(sys.argv) != 2:
        print('\nuso: python servidor.py <porta>')
        sys.exit(1)

    try:
        porta = int(sys.argv[1])
    except ValueError:
        print('\nporta inválida, informe um número inteiro')
        sys.exit(1)

    if porta <= 1024:
        print('\nporta deve ser maior que 1024')
        sys.exit(1)

    os.makedirs(DIRETORIO_BASE, exist_ok=True)

    servidor = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    servidor.bind(('', porta))
    servidor.settimeout(1.0)
    print('\nservidor escutando na porta {}'.format(porta))

    while True:
        try:
            dados, endereco = servidor.recvfrom(65536)
            tipo, valor = protocolo.desempacotar(dados)

            if tipo == protocolo.REQ:
                print('\nrequisição de {} para o arquivo "{}"'.format(endereco, valor))
                caminho, mensagem_erro = validar_req(valor)
                if caminho is None:
                    servidor.sendto(protocolo.empacotar_erro(mensagem_erro), endereco)
                else:
                    transferir_arquivo(servidor, caminho, endereco)
            else:
                servidor.sendto(protocolo.empacotar_erro('tipo de mensagem não suportado'), endereco)
        except socket.timeout:
            continue
        except Exception as erro:
            print('\nerro ao processar datagrama: {}'.format(erro))


if __name__ == '__main__':
    main()
