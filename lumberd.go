package main

import (
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"io"
	"log"
	"net"
)

func main() {
	/*cert, err := tls.LoadX509KeyPair("logstash-forwarder.crt", "logstash-forwarder.key")
	if err != nil {
		log.Fatal(err)
	}
	listener, err := tls.Listen("tcp", "127.0.0.1:5043", &tls.Config{Certificates: []tls.Certificate{cert}})
	*/
	listener, err := net.Listen("tcp", "127.0.0.1:5043")
	if err != nil {
		log.Fatal(err)
	}

	for {
		client, err := listener.Accept()
		if err != nil {
			log.Fatal("accept", err)
		}
		go func(c net.Conn) {
			buf := make([]byte, 2)

			for {
				_, err = io.ReadFull(c, buf)
				if err == io.EOF {
					log.Println("Disconnected")
					break
				}
				if err != nil || string(buf) != "1W" {
					log.Fatal(err, "Expected first header to be 1W, got", string(buf))
				}

				var nevents uint32
				err = binary.Read(c, binary.BigEndian, &nevents)
				if err != nil {
					log.Fatal("binary read after 1W", err)
				}

				log.Println(string(buf), "nevents", nevents)

				_, err = io.ReadFull(c, buf)
				if err != nil || string(buf) != "1C" {
					log.Fatal(err, "Expected compressed frame, 1C, got", string(buf))
				}

				var compressedLength uint32
				err = binary.Read(c, binary.BigEndian, &compressedLength)
				if err != nil {
					log.Fatal("reading compressedLength", err)
				}

				// TODO: Use LimitedReader, don't buffer here
				zdata := make([]byte, compressedLength)
				n, err := io.ReadFull(c, zdata)
				if err != nil {
					log.Fatal("compressed data", err, n, compressedLength)
				}

				if uint32(n) != compressedLength {
					log.Fatal("short read", n, compressedLength)
				}

				r, err := zlib.NewReader(bytes.NewReader(zdata))

				var lastSequence uint32
				for event := uint32(0); event < nevents; event++ {
					_, err := io.ReadFull(r, buf)
					if err != nil || (string(buf) != "1D" && string(buf) != "1J") {
						log.Fatal(err, " Expected data frame, 1D|1J, got ", string(buf))
					}

					var sequence uint32
					err = binary.Read(r, binary.BigEndian, &sequence)
					if err != nil {
						log.Fatal("reading sequence", err)
					}

					if string(buf) == "1J" {
						var jsonLength uint32
						err = binary.Read(r, binary.BigEndian, &jsonLength)
						if err != nil {
							log.Fatal("reading JSON length", err)
						}

						jsonData := make([]byte, jsonLength)
						_, err := io.ReadFull(r, jsonData)
						if err != nil {
							log.Fatal("reading JSON data", err)
						}

						log.Println(string(jsonData))
					} else {

						var pairCount uint32
						err = binary.Read(r, binary.BigEndian, &pairCount)
						if err != nil {
							log.Fatal("reading pair count", err)
						}

						log.Println(string(buf), sequence, pairCount)

						for p := uint32(0); p < pairCount; p++ {
							var keyLen, valueLen uint32
							err = binary.Read(r, binary.BigEndian, &keyLen)
							if err != nil {
								log.Fatal("reading key length", err)
							}

							key := make([]byte, keyLen)
							_, err := io.ReadFull(r, key)
							if err != nil {
								log.Fatal("reading key data", err)
							}

							err = binary.Read(r, binary.BigEndian, &valueLen)
							if err != nil {
								log.Fatal("reading value length", err)
							}
							value := make([]byte, valueLen)
							_, err = io.ReadFull(r, value)
							if err != nil {
								log.Fatal("reading value data", err)
							}
							log.Println(event, nevents, p, pairCount, string(key), string(value))
						}
					}
					lastSequence = sequence
				}

				c.Write([]byte("1A"))
				binary.Write(c, binary.BigEndian, lastSequence)
			}
			c.Close()
		}(client)
	}
}
