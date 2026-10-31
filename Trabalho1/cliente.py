import math
import os
import random
import socket
import sys
import time
import zlib

import protocolo

TIMEOUT_SEGUNDOS = 4
TIMEOUT_TRANSFERENCIA = 30
TIMEOUT_ENCERRAMENTO = 8.0
TOLERANCIA_SEGMENTO = 1.5

DIRETORIO_CLIENTE = os.path.abspath(os.path.join(os.path.dirname(__file__), 'arquivos_cliente'))

sys.stdin.reconfigure(encoding='utf-8')
sys.stdout.reconfigure(encoding='utf-8')


def receber_arquivo(cliente, endereco_dados, nome_arquivo, tamanho, num_segmentos, probabilidade_descarte):
    os.makedirs(DIRETORIO_CLIENTE, exist_ok=True)
    caminho_final = os.path.join(DIRETORIO_CLIENTE, nome_arquivo)
    caminho_parcial = caminho_final + '.parcial'

    proximo_esperado = 0
    bytes_recebidos = 0
    ja_decididos = set()
    completo_localmente = False
    momento_ultimo_progresso = time.monotonic() 

    cliente.settimeout(TOLERANCIA_SEGMENTO)

    with open(caminho_parcial, 'wb') as arquivo:
        while True:
            try:
                dados, _ = cliente.recvfrom(65536)
            except socket.timeout:
                if not completo_localmente:
                    if time.monotonic() - momento_ultimo_progresso >= TIMEOUT_TRANSFERENCIA:
                        print('\ntempo esgotado aguardando dados do servidor, transferência falhou')
                        return
                    cliente.sendto(protocolo.empacotar_retr(proximo_esperado), endereco_dados)
                    print('\n[RETR] segmento {} não chegou em {:.1f}s, solicitando retransmissão'.format(
                        proximo_esperado, TOLERANCIA_SEGMENTO
                    ))
                    continue
                mensagem_final = (
                    'arquivo recebido e íntegro; nenhum problema de encerramento foi observado '
                    'nos últimos {:.0f} segundos: {}'.format(TIMEOUT_ENCERRAMENTO, caminho_final)
                )
                break

            try:
                tipo, valor = protocolo.desempacotar(dados)
            except Exception:
                continue

            if tipo == protocolo.ERRO:
                if not completo_localmente:
                    print('\ntransferência interrompida pelo servidor: {}'.format(valor))
                    return
                mensagem_final = (
                    'arquivo local completo e íntegro, mas o servidor reportou falha no '
                    'encerramento: {}'.format(valor)
                )
                break

            if tipo != protocolo.DADO:
                continue

            seq, checksum, conteudo = valor

            if seq == proximo_esperado:
                if seq not in ja_decididos and random.random() < probabilidade_descarte:
                    ja_decididos.add(seq)
                    print('\n[DESCARTE] segmento {} descartado propositalmente'.format(seq))
                    continue

                if (zlib.crc32(conteudo) & 0xFFFFFFFF) != checksum:
                    cliente.sendto(protocolo.empacotar_retr(seq), endereco_dados)
                    continue

                arquivo.write(conteudo)
                cliente.sendto(protocolo.empacotar_ack(seq), endereco_dados)
                proximo_esperado += 1
                bytes_recebidos += len(conteudo)
                momento_ultimo_progresso = time.monotonic()

                if proximo_esperado == num_segmentos and bytes_recebidos == tamanho:
                    arquivo.flush()
                    completo_localmente = True
                    cliente.settimeout(TIMEOUT_ENCERRAMENTO)
            elif seq < proximo_esperado:
                cliente.sendto(protocolo.empacotar_ack(seq), endereco_dados)
                if completo_localmente:
                    cliente.settimeout(TIMEOUT_ENCERRAMENTO)

    os.replace(caminho_parcial, caminho_final)
    print('\n' + mensagem_final)


def ler_probabilidade_descarte():
    while True:
        entrada = input('\nDigite a probabilidade de descarte proposital (0 a 1): ')
        try:
            probabilidade = float(entrada)
        except ValueError:
            print('\nprobabilidade inválida, informe um número entre 0 e 1')
            continue
        if not math.isfinite(probabilidade):
            print('\nprobabilidade inválida, informe um número entre 0 e 1')
            continue
        if probabilidade < 0.0 or probabilidade > 1.0:
            print('\nprobabilidade inválida, informe um número entre 0 e 1')
            continue
        return probabilidade


def main():
    if len(sys.argv) != 3:
        print('\nuso: python cliente.py <ip_servidor> <porta_servidor>')
        sys.exit(1)

    ip_servidor = sys.argv[1]

    try:
        porta_servidor = int(sys.argv[2])
    except ValueError:
        print('\nporta inválida, informe um número inteiro')
        sys.exit(1)

    nome_arquivo = input('\nDigite o nome do arquivo desejado: ')
    
    probabilidade_descarte = ler_probabilidade_descarte()
    
    cliente = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    cliente.settimeout(TIMEOUT_SEGUNDOS)

    requisicao = protocolo.empacotar_req(nome_arquivo)
    cliente.sendto(requisicao, (ip_servidor, porta_servidor))

    try:
        dados, endereco_servidor = cliente.recvfrom(65536)
    except (socket.timeout, ConnectionResetError):
        print('\nservidor não respondeu, verifique IP/porta')
        cliente.close()
        sys.exit(1)

    tipo, valor = protocolo.desempacotar(dados)

    if tipo == protocolo.META:
        tamanho, num_segmentos, porta_dados = valor
        print('\narquivo disponível, tamanho: {} bytes, {} segmento(s)'.format(tamanho, num_segmentos))
        endereco_dados = (ip_servidor, porta_dados)
        receber_arquivo(cliente, endereco_dados, nome_arquivo, tamanho, num_segmentos, probabilidade_descarte)
    elif tipo == protocolo.ERRO:
        print('\no servidor recusou o pedido: {}'.format(valor))
    else:
        print('\nresposta inesperada do servidor')
    
    cliente.close()


if __name__ == '__main__':
    main()
