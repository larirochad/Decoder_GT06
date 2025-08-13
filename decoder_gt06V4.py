# decoder_gt06V4.py
from recordMessages import * 
from datetime import timedelta

def decode_course_info(course_hex):
    course_MSB = int(course_hex[0:2], 16)
    course_LSB = int(course_hex[2:4], 16)

    msb_bin = bin(course_MSB)[2:].zfill(8)  # Binário com 8 bits
    lsb_bin = bin(course_LSB)[2:].zfill(8)  # Binário com 8 bits

    """
    Baseado no protocolo GT06V4 oficial:
    BYTE_1 (bits 15-8):
    - Bit 7: 0 (reservado)
    - Bit 6: 0 (reservado)  
    - Bit 5: GPS real-time/differential positioning (0=realtime, 1=differential)
    - Bit 4: GPS having been positioning or not (1=positioned, 0=not positioned)
    - Bit 3: East/West Longitude (0=East/+, 1=West/-)
    - Bit 2: North/South Latitude (0=South/-, 1=North/+)
    - Bits 1-0: Course bits altos
    
    BYTE_2 (bits 7-0): Course bits baixos (0-360°)
    """
    course_bin = msb_bin + lsb_bin  # Concatena os dois bytes em binário
    # print(f"course_bin: {course_bin}")  # Debug: Exibe o binário completo
    # BYTE_1 (bits 15-8) - primeiro byte
    realtime_gps = int(course_bin[2])      # Bit 5 ✔️
    gps_posicionado = int(course_bin[3])   # Bit 4 ✔️
    longitude_leste = int(course_bin[4])   # Bit 3 ✔️
    latitude_norte = int(course_bin[5])    # Bit 2 ✔️

    course_bits =   course_bin[6:16]  
    # print(f"azimulth em bin: {course_bits}")  # Debug: Exibe os bits do curso
    azimute = int(course_bits, 2)  # Conversão direta para graus (0-360°)
    
    return {
        'azimute': azimute,
        'realtime_gps': realtime_gps,
        'gps_posicionado': gps_posicionado,
        'longitude_leste': longitude_leste,
        'latitude_norte': latitude_norte,
        'course_bin': course_bin,
        'course_bits': course_bits
    }


def apply_coordinate_signs(latitude, longitude, course_info):
    """
    Aplica os sinais corretos à latitude e longitude baseado nos bits do course
    """
    # Aplicar sinal da latitude (bit 2): 1=Norte(+), 0=Sul(-)
    if course_info['latitude_norte'] == 0: 
        latitude = -latitude
    
    # Aplicar sinal da longitude (bit 3): 0=Leste(+), 1=Oeste(-)  
    if course_info['longitude_leste'] == 1:
        longitude = -longitude
        
    return latitude, longitude

def parser_gt06V4(hex_data, imei=None):
    protocol_number = hex_data[6:8]

    try:
        if protocol_number == "01":  # Login
            print("\n Login")
            Tipo_mensagem = "Login"
            inicio = hex_data[0:4]
            p = 4
            length = hex_data[p:p+2]
            p += 2
            protocol_number = hex_data[p:p+2]
            p += 2
            imei = hex_data[p:p+16]
            p += 16
            serial_number = hex_data[p:p+4]
            serial_number = int(serial_number, 16)
            p += 4
            checksum = hex_data[p:p+4]
            p += 4
            tail = hex_data[p:p+4]

            print(f"# Login Message")
            print(f"head: {inicio}")                           
            print(f"length: {int(length, 16)} ({length})")    
            print(f"protocol_number: {protocol_number}")      
            print(f"imei: {imei}")                         
            print(f"Count Number:   {serial_number}")         
            print(f"checksum: {checksum}")                   
            print(f"tail: {tail}")                           

            linha = f",{imei},{serial_number},{Tipo_mensagem},77,GT06V4,,,," \
                    f",,,,,,," \

            # Grava no arquivo CSV
            record_decoded_organized(imei, linha) 


        elif protocol_number == "13":  # Heartbeat
            print("\n Heartbeat")
            Tipo_mensagem = "Heartbeat"
            inicio = hex_data[0:4]
            p = 4
            length = hex_data[p:p+2]
            p += 2
            protocol_number = hex_data[p:p+2]
            p += 2
            terminal_info = hex_data[p:p+2]
            p += 2
            external_power = hex_data[p:p+2]
            p += 2
            gsm_signal = hex_data[p:p+2]
            p += 2
            alarm = hex_data[p:p+4]
            p += 4
            serial_number = hex_data[p:p+4]
            serial_number = int(serial_number, 16)
            p += 4
            checksum = hex_data[p:p+4]
            p += 4
            tail = hex_data[p:p+4]
            p += 4
            
            external_power = str(external_power)
           
            if external_power == '00':
                external_power = "Sem bateria"
            elif external_power == '01':
                external_power = "Bateria extremamente baixa"
            elif external_power == '02':
                external_power = "Bem baixa bateria"
            elif external_power == '03':
                external_power = "Bateria baixa"
            elif external_power == '04':
                external_power = "Bateria média"
            elif external_power == '05':
                external_power = "Bateria alta"
            elif external_power == '06':
                external_power = "Bateria extremamente alta"
            else: 
                external_power = "Desconhecido"

            print(f"Head: {inicio}")
            print(f"Length: {int(length, 16)} ({length})")
            print(f"Protocol_number: {protocol_number}")
            print(f"Terminal_info: {terminal_info}")
            print("Bateria: " + str(external_power))
            print(f"GSM_signal: {gsm_signal}")
            print(f"Alarm: {alarm}")
            print("Count Number: " + str({serial_number}))
            print(f"Checksum: {checksum}")
            print(f"Tail: {tail}")

            linha = f",{imei},{serial_number},{Tipo_mensagem},77,GT06V4,,,{external_power}," \
                    f",,,,,,,,,,,,,,,,{gsm_signal}" \

            # Grava no arquivo CSV
            record_decoded_organized(imei, linha) 
       
        elif protocol_number == "32":  # GPS Data
            print("\nTemporizadas")
            inicio = hex_data[0:4]
            p = 4
            length = hex_data[p:p+2]
            p += 2 
            protocol_number = hex_data[p:p+2]
            p += 2  
            send_time = hex_data[p:p+12]
            send_time_utc = hex_to_timestamp(send_time)
            p += 12 
            gps = hex_data[p:p+2]
            satelites_in_use = int(str(gps[1]), 16)
   
            p += 2
            latitude = hex_data[p:p+8]
            latitude = int(latitude, 16) / 1800000  # Convertendo para graus
            p += 8
            longitude = hex_data[p:p+8]
            longitude = int(longitude, 16) / 1800000  # Convertendo para graus
            p += 8
            speed = hex_data[p:p+2]
            speed = int(speed, 16) 
            p += 2
            
            course = hex_data[p:p+4]
            # print(f"course: {course}")
            course_info = decode_course_info(course)
            
            latitude, longitude = apply_coordinate_signs(latitude, longitude, course_info)
            
            p += 4
            mcc = hex_data[p:p+4]
            p += 4
            mnc = hex_data[p:p+2]
            p += 2
            lac = hex_data[p:p+4]   
            p += 4
            cell_id = hex_data[p:p+8]
            p += 8
            acc = hex_data[p:p+2]
            acc = int(acc, 16) 
            p += 2
            data_up = hex_data[p:p+2]
            p += 2
            gps_real = hex_data[p:p+2]
            p += 2
            milage = hex_data[p:p+8]
            milage = int(milage, 16) / 1000  # Convertendo para km
            p += 8
            external_power = hex_data[p:p+4]
            external_power = int(external_power, 16)* 0.01
            external_power = float(external_power)
            external_power_str = f"{external_power:.2f}" 
            p += 4
            acc_on_time = hex_data[p:p+8]
            acc_on_time = int(acc_on_time, 16) 
            tempo = timedelta(seconds=acc_on_time)
            dias = tempo.days
            horas, resto = divmod(tempo.seconds, 3600)
            minutos, segundos = divmod(resto, 60)
            tempo_formatado = f"{dias:02d}-{horas:02d}:{minutos:02d}:{segundos:02d}"

            p += 8
            rat = hex_data[p:p+4]
            p += 4
            rat_prefix = rat[:1]
            rat_suffix = int(rat[1:], 16)
            serial_number = hex_data[p:p+4]
            serial_number = int(serial_number, 16) 
            p += 4
            checksum = hex_data[p:p+4]
            p += 4
            tail = hex_data[p:p+4]
            p += 4
            Tipo_mensagem = "Temporizada"
            
            print("Head:" + inicio) 
            print("Length: " + str(int(length, 16)))
            print("Protocol_number: " + protocol_number)    
            print("Send_time (BR): " + converter_para_brasil(send_time_utc))
            print("Satélites: " + str(satelites_in_use))
            print(f"Latitude: {latitude:.6f}")
            print(f"Longitude: {longitude:.6f} ")
            print("Speed: " + str(speed))
            print(f"Course: {course} (Azimute: {course_info['azimute']}°)")
            print(f"GPS Info Realtime: " + str(course_info['realtime_gps']))
            print(f"Posicionado: " + str(course_info['gps_posicionado']))
            print("MCC: " + mcc)
            print("MNC: " + mnc)
            print("lac: " + lac)
            print("cell_id: " + cell_id)    
            print("acc: " + str(acc))
            print("data_up: " + data_up)
            print("gps_real: " + gps_real)
            print("Serial number: " + str(serial_number))
            print("milage: " + str(milage))
            print("external_power: " + str(external_power_str))
            print("acc_on_time: " + str(tempo_formatado))
            print("Tipo de rede: " + rat_prefix)
            print("Versão de Firmware: " + str(rat_suffix))
            print("checksum: " + checksum)
            print("tail: " + tail)

             # Prepara linha formatada para CSV
            # data_inclusao = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            linha = f"{converter_para_brasil(send_time_utc)},{imei},{serial_number},{Tipo_mensagem},77,GT06V4,{rat_suffix},{external_power_str},," \
                    f"{acc},{satelites_in_use},,{speed},{course_info['azimute']},{latitude:.6f},{longitude:.6f},{mcc},{mnc},{lac},{cell_id},{course_info['realtime_gps']},{course_info['gps_posicionado']},{milage},{tempo_formatado},{rat_prefix}," \

            # Grava no arquivo CSV
            record_decoded_organized(imei, linha) 

            
        elif protocol_number == "16":  # GPS Data with additional info
            print("\n Alarm")
            inicio = hex_data[0:4]
            p = 4  
            length = hex_data[p:p+2]
            p += 2  
            protocol_number = hex_data[p:p+2]
            p += 2
            send_time = hex_data[p:p+12]
            send_time_utc = hex_to_timestamp(send_time)
            p += 12
            gps = hex_data[p:p+2]
            satelites_in_use = int(str(gps[0]), 16)
            p += 2
            latitude = hex_data[p:p+8]
            latitude = int(latitude, 16) / 1800000  # Convertendo para graus
            p += 8
            longitude = hex_data[p:p+8]
            longitude = int(longitude, 16) / 1800000  # Convertendo para graus
            p += 8
            speed = hex_data[p:p+2]
            speed = int(speed, 16)
            p += 2
            
            # NOVA IMPLEMENTAÇÃO DO COURSE PARA ALARM
            course = hex_data[p:p+4]
            course_info = decode_course_info(course)
            
            # Aplicar sinais corretos nas coordenadas
            latitude, longitude = apply_coordinate_signs(latitude, longitude, course_info)
            
            p += 4
            LBS_Len = hex_data[p:p+2]
            p += 2
            mcc = hex_data[p:p+4]
            p += 4
            mnc = hex_data[p:p+2]
            p += 2
            lac = hex_data[p:p+4]
            p += 4
            cell_id = hex_data[p:p+6]
            p += 6
            terminal_status = hex_data[p:p+2]
            p += 2
            external_power = hex_data[p:p+2]
            p += 2
            gsm_signal = hex_data[p:p+2]
            p += 2
            alarm = hex_data[p:p+4] 
            p += 4
            milage = hex_data[p:p+8]
            milage = int(milage, 16) / 1000
            p += 8
            serial_number = hex_data[p:p+4]
            serial_number = int(serial_number, 16) 
            p += 4
            checksum = hex_data[p:p+4]  
            p += 4
            tail = hex_data[p:p+4]
            p += 4
            
            alarm_str = str(alarm)
            #print(f"alarm: {alarm_str}")
            alarm_prefix = alarm_str[:2]
            if alarm_prefix == "01":
                Tipo_mensagem = "Alerta de Pânico"
                print("Panic alarm")
            elif alarm_prefix == "02":
                Tipo_mensagem = "Desconexão de bateria"
                print("Desconexão de bateria")
            elif alarm_prefix == "06":
                Tipo_mensagem = "Exesso de velocidade"
                print("Excesso de velocidade")
            elif alarm_prefix == "16":
                Tipo_mensagem = "Retorno de velocidade"
                print("retorno de velocidade")
            elif alarm_prefix == "F2":
                Tipo_mensagem = "Suspeita de acidente"
                print("Suspeita de acidente")
            elif alarm_prefix == "F3":
                Tipo_mensagem = "Bloqueio"
                print("Bloqueio")
            elif alarm_prefix == "F4":  
                Tipo_mensagem = "Desbloqueio"
                print("Desbloqueio")
            elif alarm_prefix == "FE":
                Tipo_mensagem = "IGN"
                print("GTIGN")
            elif alarm_prefix == "FF":
                Tipo_mensagem = "IGF"
                print("GTIGF")

            external_power = str(external_power)
            if external_power == '00':
                external_power = "Sem bateria"
            elif external_power == '01':
                external_power = "Bateria extremamente baixa"
            elif external_power == '02':
                external_power = "Bem baixa bateria"
            elif external_power == '03':
                external_power = "Bateria baixa"
            elif external_power == '04':
                external_power = "Bateria média"
            elif external_power == '05':
                external_power = "Bateria alta"
            elif external_power == '06':
                external_power = "Bateria extremamente alta"
            else: 
                external_power = "Desconhecido"

            print("Head: " + inicio)    
            print("Length: " + str(int(length, 16)) + " (" + length + ")")
            print("Protocol_number: " + protocol_number)
            print("Send_time (BR): " + converter_para_brasil(send_time_utc))
            print("Satélites: " + str(satelites_in_use))
            print(f"Latitude: {latitude:.6f}" )
            print(f"Longitude: {longitude:.6f}")
            print("Speed: " + str(speed))
            print(f"Course: {course} (Azimute: {course_info['azimute']}°)")
            print(f"GPS Info Realtime: " + str(course_info['realtime_gps']))
            print(f"Posicionado: " + str(course_info['gps_posicionado']))
            print("LBS_Len: " + LBS_Len)
            print("MCC: " + mcc)
            print("MNC: " + mnc)
            print("LAC: " + lac)
            print("Cell_id: " + cell_id)
            print("Terminal_status: " + terminal_status)
            print("External_power: " + str(external_power))
            print("GMS_signal: " + gsm_signal)
            print("Milage: " + str(milage))
            print("Count Number: " + str(serial_number))
            print("Checksum: " + checksum)    
            print("Tail: " + tail)

             # Prepara linha formatada para CSV
            # data_inclusao = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            linha = f"{converter_para_brasil(send_time_utc)},{imei},{serial_number},{Tipo_mensagem},77,GT06V4,,,{external_power},," \
                    f"{satelites_in_use},,{speed},{course_info['azimute']},{latitude:.6f},{longitude:.6f},{mcc},{mnc},{lac},{cell_id},{course_info['realtime_gps']},{course_info['gps_posicionado']},{milage},,," \
 
            # Grava no arquivo CSV
            record_decoded_organized(imei, linha)

        elif protocol_number == "15":  
            print("\nAck comando")
            inicio = hex_data[0:4]
            p = 4
            length = hex_data[p:p+2]
            p += 2
            protocol_number = hex_data[p:p+2]
            p += 2
            length_comando = hex_data[p:p+2]
            p += 2
            server_flag = hex_data[p:p+8]
            p += 8
            resposta = hex_data[p:]
            resposta_ascii = bytes.fromhex(resposta).decode('ascii', errors='ignore')

            print(f"\nHead: {inicio}")
            print(f"Length: {int(length, 16)} ({length})")  
            print(f"Protocol_number: {protocol_number}")
            print(f"Length comando: {int(length_comando, 16)} ({length_comando})")
            print(f"Server flag: {server_flag}")
            print(f"Resposta: {resposta_ascii}")


        else:   
            print(f"Protocolo {protocol_number} ainda não implementado.")
            return None
        
    except Exception as e:
        print(f"Erro ao processar dados: {str(e)}")
        return None