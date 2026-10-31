import struct

REQ = 0
META = 1
ERRO = 2
DADO = 3
ACK = 4
RETR = 5

TAMANHO_DADOS = 1391


def empacotar_req(nome_arquivo):
    return bytes([REQ]) + nome_arquivo.encode('utf-8')


def empacotar_meta(tamanho, num_segmentos, porta_dados):
    return bytes([META]) + struct.pack('!QIH', tamanho, num_segmentos, porta_dados)


def empacotar_erro(mensagem):
    return bytes([ERRO]) + mensagem.encode('utf-8')


def empacotar_dado(seq, checksum, dados):
    return bytes([DADO]) + struct.pack('!II', seq, checksum) + dados


def empacotar_ack(seq):
    return bytes([ACK]) + struct.pack('!I', seq)


def empacotar_retr(seq):
    return bytes([RETR]) + struct.pack('!I', seq)


def desempacotar(dados):
    tipo = dados[0]
    payload = dados[1:]
    if tipo == REQ:
        return REQ, payload.decode('utf-8')
    if tipo == META:
        tamanho, num_segmentos, porta_dados = struct.unpack('!QIH', payload)
        return META, (tamanho, num_segmentos, porta_dados)
    if tipo == ERRO:
        return ERRO, payload.decode('utf-8')
    if tipo == DADO:
        seq, checksum = struct.unpack('!II', payload[:8])
        return DADO, (seq, checksum, payload[8:])
    if tipo == ACK:
        seq, = struct.unpack('!I', payload)
        return ACK, seq
    if tipo == RETR:
        seq, = struct.unpack('!I', payload)
        return RETR, seq
    raise ValueError('Tipo de mensagem desconhecido: {}'.format(tipo))
