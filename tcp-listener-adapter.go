package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	mqtt "github.com/clearblade/paho.mqtt.golang"
	"github.com/hashicorp/logutils"

	cb "github.com/clearblade/Go-SDK"
)

const (
	msgPublishQOS              = 0
	defaultTopicRoot           = "tcp-listener"
	defaultListenPort          = "12345"
	defaultMessageEndCharacter = ""
	defaultDeviceName          = "tcp-listener-adapter"
	defaultPlatformURL         = "http://localhost:9000"
	defaultMessagingURL        = "localhost:1883"
	defaultLogLevel            = "info"
	defaultLogFilePath         = "/var/log/tcp-listener-adapter.log"
)

var (
	sysKey              string
	sysSec              string
	deviceName          string
	activeKey           string
	platformURL         string
	messagingURL        string
	logLevel            string
	logFilePath         string
	adapterConfigCollID string
	cbClient            *cb.DeviceClient
	config              adapterConfig
)

type adapterSettings struct {
	ListenPort          string `json:"listen_port"`
	MessageEndCharacter string `json:"message_end_character"`
}

type adapterConfig struct {
	AdapterSettings adapterSettings `json:"adapter_settings"`
	TopicRoot       string          `json:"topic_root"`
}

func init() {
	flag.StringVar(&sysKey, "systemKey", "", "system key (required)")
	flag.StringVar(&sysSec, "systemSecret", "", "system secret (required)")
	flag.StringVar(&deviceName, "deviceName", defaultDeviceName, "name of device (optional)")
	flag.StringVar(&activeKey, "activeKey", "", "active key for device authentication (required)")
	flag.StringVar(&platformURL, "platformURL", defaultPlatformURL, "platform url (optional)")
	flag.StringVar(&messagingURL, "messagingURL", defaultMessagingURL, "messaging URL (optional)")
	flag.StringVar(&logLevel, "logLevel", defaultLogLevel, "The level of logging to use. Available levels are 'debug, 'info', 'warn', 'error', 'fatal' (optional)")
	flag.StringVar(&logFilePath, "logFilePath", defaultLogFilePath, "Path for the log file of the adapter (optional - use special case of stdout if you don't want to log to a file)")
	flag.StringVar(&adapterConfigCollID, "adapterConfigCollectionID", "", "The ID of the data collection used to house adapter configuration (required)")
}

func usage() {
	log.Printf("Usage: tcp-listener-adapter [options]\n\n")
	flag.PrintDefaults()
}

func validateFlags() {
	flag.Parse()

	if sysKey == "" || sysSec == "" || activeKey == "" || adapterConfigCollID == "" {
		log.Println("ERROR - Missing required flags")
		flag.Usage()
		os.Exit(1)
	}

}

func main() {
	log.Println("Starting tcp-listener-adapter...")

	flag.Usage = usage
	validateFlags()

	rand.Seed(time.Now().UnixNano())

	log.SetFlags(log.LstdFlags | log.Lshortfile)
	filter := &logutils.LevelFilter{
		Levels:   []logutils.LogLevel{"DEBUG", "INFO", "WARN", "ERROR", "FATAL"},
		MinLevel: logutils.LogLevel(strings.ToUpper(logLevel)),
	}
	if logFilePath == "stdout" {
		log.Println("using stdout for logging")
		filter.Writer = os.Stdout
	} else {
		log.Printf("using %s for logging\n", logFilePath)
		logfile, err := os.OpenFile(logFilePath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
		if err != nil {
			log.Fatalf("Failed to open log file: %s", err.Error())
		}
		defer logfile.Close()
		filter.Writer = logfile
	}
	log.SetOutput(filter)

	initClearBlade()
	initAdapterConfig()
	connectClearBlade()

	log.Println("[DEBUG] main - starting info log ticket")

	ticket := time.NewTicker(60 * time.Second)
	defer ticket.Stop()
	for {
		select {
		case <-ticket.C:
			log.Println("[INFO] main - Listening for incoming data on TCP socket")
		}
	}

}

func initClearBlade() {
	log.Println("[DEBUG] initClearBlade - initializing ClearBlade")
	cbClient = cb.NewDeviceClientWithAddrs(platformURL, messagingURL, sysKey, sysSec, deviceName, activeKey)
	for err := cbClient.Authenticate(); err != nil; {
		log.Printf("[ERROR] initClearBlade - Error authenticating %s: %s\n", deviceName, err.Error())
		log.Println("[INFO] initClearBlade - Trying again in 1 minute...")
		time.Sleep(time.Minute * 1)
		err = cbClient.Authenticate()
	}
	log.Println("[INFO] initClearBlade - ClearBlade successfully initialized")
}

func initAdapterConfig() {
	log.Println("[DEBUG] initAdapterConfig - loading adapter config from collection")
	config = adapterConfig{
		TopicRoot: defaultTopicRoot,
		AdapterSettings: adapterSettings{
			ListenPort:          defaultListenPort,
			MessageEndCharacter: defaultMessageEndCharacter,
		},
	}
	query := cb.NewQuery()
	query.EqualTo("adapter_name", deviceName)
	results, err := cbClient.GetData(adapterConfigCollID, query)
	if err != nil {
		log.Printf("[ERROR] initAdapterConfig - failed to fetch adapter config: %s\n", err.Error())
		log.Printf("[INFO] initAdapterConfig - using default adapter config: %+v\n", config)
	} else {
		data := results["DATA"].([]interface{})
		if len(data) == 1 {
			configData := data[0].(map[string]interface{})
			if configData["topic_root"] != nil {
				config.TopicRoot = configData["topic_root"].(string)
				log.Printf("[INFO] initAdapterConfig - using topic root from adapter config collection: %s\n", config.TopicRoot)
			}
			if configData["adapter_settings"] != nil {
				var adpSet adapterSettings
				if err := json.Unmarshal([]byte(configData["adapter_settings"].(string)), &adpSet); err != nil {
					log.Printf("[ERROR] initAdapterConfig - failed to unmarshal adapter_settings json: %s\n", err.Error())
					log.Printf("[INFO] initAdapterConfig - using default adapter settings for config: %+v\n", config)
				} else {
					config.AdapterSettings = adpSet
					log.Printf("[INFO] initAdapterConfig - using adapter settings from adapter config collection: %+v\n", config.AdapterSettings)
				}
			}
		} else {
			log.Printf("[ERROR] initAdapterConfig - Unexpected number of matching adapter configs: %d\n", len(data))
			log.Printf("[INFO] initAdapterConfig - using default adapter config: %+v\n", config)
		}
	}
	log.Println("[INFO] initAdapterConfig - adapter config successfully loaded")
}

func connectClearBlade() {
	log.Println("[INFO] connectClearBlade - connecting ClearBlade MQTT")
	callbacks := cb.Callbacks{OnConnectCallback: onConnect, OnConnectionLostCallback: onConnectLost}
	if err := cbClient.InitializeMQTTWithCallback(deviceName+"-"+strconv.Itoa(rand.Intn(10000)), "", 30, nil, nil, &callbacks); err != nil {
		log.Fatalf("[FATAL] connectClearBlade - Unable to connect ClearBlade MQTT: %s", err.Error())
	}
}

func onConnect(client mqtt.Client) {
	log.Println("[INFO] onConnect - ClearBlade MQTT successfully connected")
	go createTCPListener()
}

func onConnectLost(client mqtt.Client, connerr error) {
	log.Printf("[ERROR] onConnectLost - ClearBlade MQTT lost connection: %s", connerr.Error())
	// reconnect logic should be handled by go/paho sdk under the covers
}

func createTCPListener() {
	log.Println("[INFO] createTCPListener - Creating TCP Listener")
	listener, err := net.Listen("tcp", ":"+config.AdapterSettings.ListenPort)
	if err != nil {
		log.Fatalf("[FATAL] createTCPListener - Error Creating TCP Listener: %s", err.Error())
	}

	log.Println("[INFO] createTCPListener - Listener opened and ready to accecpt connections")

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("[ERROR] createTCPListener - Failed to accept new connection: %s\n", err.Error())
			break
		}
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	log.Println("[INFO] handleConnection - New TCP connection accepted")

	defer func() {
		log.Println("[INFO] handleConnection - closing TCP connection")
		conn.Close()
	}()

	if config.AdapterSettings.MessageEndCharacter == "" {
		log.Println("[INFO] handleConnection - Reading all data until TCP connection is closed...")
		bytes, err := ioutil.ReadAll(conn)
		if err != nil {
			log.Printf("[ERROR] handleConnection - Failed to read data from TCP connection: %s\n", err.Error())
			return
		}
		publishMessage(string(bytes[:]))
	} else {
		log.Printf("[INFO] handleConnection - Reading all data until character %s\n", config.AdapterSettings.MessageEndCharacter)
		scanner := bufio.NewScanner(bufio.NewReader(conn))
		split := func(data []byte, atEOF bool) (advance int, token []byte, err error) {
			if atEOF && len(data) == 0 {
				return 0, nil, nil
			}

			if i := strings.Index(string(data), config.AdapterSettings.MessageEndCharacter); i >= 0 {
				return i + 1, data[0:i], nil
			}

			if atEOF {
				return len(data), data, nil
			}

			return
		}
		scanner.Split(split)
		for scanner.Scan() {
			log.Println("[INFO] handleConnection - Read line of data")
			publishMessage(scanner.Text())
		}
		if err := scanner.Err(); err != nil {
			log.Printf("[ERROR] handleConnection - Failed to read data from TCP connection: %s\n", err.Error())
			return
		}

	}
}

func publishMessage(msg string) {
	if err := cbClient.Publish(config.TopicRoot+"/incoming-data", []byte(msg), msgPublishQOS); err != nil {
		log.Printf("[ERROR] publishMessage - Failed to publish message: %s\n", err.Error())
	}
}

// package main

// import (
// 	"bufio"
// 	"flag"
// 	cb "github.com/clearblade/Go-SDK"
// 	"io/ioutil"
// 	"log"
// 	"net"
// 	"os"
// 	"strconv"
// 	"strings"
// )

// const (
// 	defaultListenPort = "12345"
// 	defaultTopicName  = "tcpadapter/in"
// )

// var (
// 	platformURL         string
// 	messagingURL        string
// 	systemKey           string
// 	systemSecret        string
// 	deviceName          string
// 	activeKey           string
// 	listenPort          string
// 	messageEndCharacter string
// 	topicName           string
// 	deviceClient        *cb.DeviceClient
// )

// func init() {
// 	flag.StringVar(&platformURL, "platformURL", "", "ClearBlade Platform URL (required)")
// 	flag.StringVar(&messagingURL, "messagingURL", "", "ClearBlade Platform Messaging URL (optional - will default to <platformURL>:1883)")
// 	flag.StringVar(&systemKey, "systemKey", "", "ClearBlade System Key (required)")
// 	flag.StringVar(&systemSecret, "systemSecret", "", "ClearBlade System Secret (required)")
// 	flag.StringVar(&deviceName, "deviceName", "", "Device Name to authenticate with the ClearBlade System (required)")
// 	flag.StringVar(&activeKey, "activeKey", "", "Active Key to use for authentication with the ClearBlade System (required)")
// 	flag.StringVar(&listenPort, "listenPort", defaultListenPort, "Port to listen on for new TCP connections")
// 	flag.StringVar(&messageEndCharacter, "messageEndCharacter", "", "Character used to seperate messages (defaults to none)")
// 	flag.StringVar(&topicName, "topicName", defaultTopicName, "MQTT Topic to publishing incoming messages on")
// }

// func usage() {
// 	log.Printf("Usage: tcp-adapter [options]\n\n")
// 	flag.PrintDefaults()
// }

// func validateFlags() {
// 	flag.Parse()
// 	if platformURL == "" || systemKey == "" || systemSecret == "" || deviceName == "" || activeKey == "" {
// 		log.Println("Missing a required flag")
// 		flag.Usage()
// 		os.Exit(1)
// 	}

// 	if _, err := strconv.Atoi(listenPort); err != nil {
// 		log.Println("listenPort flag must be numberic")
// 		flag.Usage()
// 		os.Exit(1)
// 	}

// 	//set default messagingURL if needed
// 	if messagingURL == "" {
// 		messagingURL = strings.TrimPrefix(platformURL, "http://")
// 		messagingURL = strings.TrimPrefix(platformURL, "https://")
// 		messagingURL += ":1883"
// 	}

// }

// func handleConnection(conn net.Conn) {
// 	log.Println("Handling New TCP Connection..")

// 	defer func() {
// 		log.Println("Closing TCP Connection..")
// 		conn.Close()
// 	}()

// 	if messageEndCharacter == "" {
// 		log.Println("Reading all data until TCP connection is closed...")
// 		bytes, err := ioutil.ReadAll(conn)
// 		if err != nil {
// 			log.Printf("Failed to read data from TCP connection: %s\n", err.Error())
// 			return
// 		}
// 		publishMessage(string(bytes[:]))
// 	} else {
// 		log.Printf("Reading all data until character %s\n", messageEndCharacter)
// 		scanner := bufio.NewScanner(bufio.NewReader(conn))
// 		split := func(data []byte, atEOF bool) (advance int, token []byte, err error) {
// 			if atEOF && len(data) == 0 {
// 				return 0, nil, nil
// 			}

// 			if i := strings.Index(string(data), messageEndCharacter); i >= 0 {
// 				return i + 1, data[0:i], nil
// 			}

// 			if atEOF {
// 				return len(data), data, nil
// 			}

// 			return
// 		}
// 		scanner.Split(split)
// 		for scanner.Scan() {
// 			publishMessage(scanner.Text())
// 		}
// 		if err := scanner.Err(); err != nil {
// 			log.Printf("Failed to read data from TCP connection: %s\n", err.Error())
// 			return
// 		}

// 	}

// }

// func determineTopic(msg string) string {
// 	return topicName
// }

// func publishMessage(msg string) {
// 	if err := deviceClient.Publish(determineTopic(msg), []byte(msg), 2); err != nil {
// 		log.Printf("Failed to publish message: %s\n", err.Error())
// 	}
// }

// func main() {
// 	flag.Usage = usage
// 	validateFlags()

// 	deviceClient = cb.NewDeviceClient(systemKey, systemSecret, deviceName, activeKey)

// 	if platformURL != "" {
// 		log.Printf("Setting Platform URL: %s\n", platformURL)
// 		deviceClient.HttpAddr = platformURL
// 	}

// 	if messagingURL != "" {
// 		log.Printf("Setting Messaging URL: %s\n", messagingURL)
// 		deviceClient.MqttAddr = messagingURL
// 	}

// 	log.Printf("Authenticating to ClearBlade platform as device: %s\n", deviceName)

// 	if err := deviceClient.Authenticate(); err != nil {
// 		log.Fatalf("Failed to authenticate: %s", err.Error())
// 	}

// 	log.Println("Initializing MQTT...")

// 	if err := deviceClient.InitializeMQTT("tcp-adapter_"+deviceName, "", 30, nil, nil); err != nil {
// 		log.Fatalf("Failed to initialize MQTT: %s", err.Error())
// 	}

// 	log.Printf("MQTT initialized, opening TCP listener on port %s\n", listenPort)

// 	listener, err := net.Listen("tcp", ":"+listenPort)
// 	if err != nil {
// 		log.Fatalf("Error Creating TCP Listener: %s", err.Error())
// 	}

// 	log.Println("Listener opened and ready to accecpt connections")

// 	for {
// 		conn, err := listener.Accept()
// 		if err != nil {
// 			log.Fatalf("Failed to accept new connection: %s\n", err.Error())
// 		}
// 		go handleConnection(conn)
// 	}
// }
