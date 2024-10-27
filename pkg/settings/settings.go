package settings

import (
	"errors"
	"flag"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/IBM/sarama"
)

type Setting struct {
	BootstrapServerPtr      *string
	BootstrapServer         []string
	Username                *string
	Passwd                  *string
	KafkaApiVersion         *string
	KafkaApiVersionFormated sarama.KafkaVersion
	Timeout                 time.Duration
}

func (s *Setting) GetSettings() {
	const sep string = ","
	s.BootstrapServerPtr = flag.String(
		"bootstrap-server",
		"127.0.0.1:9092",
		"Bootstrap server anf port (kafka1:9092[,kafka2:9092...]) of kafka",
	)
	s.Username = flag.String(
		"user",
		"",
		"User name for connect to cluster(str)",
	)
	s.Passwd = flag.String(
		"password",
		"",
		"User name for connect to cluster(str)",
	)

	s.KafkaApiVersion = flag.String(
		"api-version",
		"2.7.0",
		"--api-version seted version of brokers",
	)
	duration := flag.String(
		"timeout",
		"10000",
		"--timeout it time to out of consume message if no new message in topic. Default is 10000 ms",
	)

	flag.Parse()
	s.parsingBrokers(sep)
	s.getTimeout(*duration)
	s.getKafkaVersion()
}

func (s *Setting) parsingBrokers(separator string) {
	t := strings.Split(*s.BootstrapServerPtr, separator)
	s.BootstrapServer = append(s.BootstrapServer, t...)
}

func (s *Setting) getTimeout(t string) {
	t_tmp, err := strconv.Atoi(t)
	if err != nil {
		panic(err)
	}

	s.Timeout = time.Duration(t_tmp) * time.Millisecond
}

func (s *Setting) getKafkaVersion() {
	var (
		err error
	)
	s.KafkaApiVersionFormated, err = sarama.ParseKafkaVersion(*s.KafkaApiVersion)
	if err != nil {
		fmt.Printf("Error parsing broker api version: %v.\n\tSupported version: %v", err, sarama.SupportedVersions)
		panic("")
	}
}

// VerifyConf() returning error if one or any args incorrect
func (s Setting) VerifyConf() error {

	if len(*s.Username) > 0 {
		if len(*s.Passwd) == 0 {
			return errors.New("password is not set. -h or --help for more details")
		}
	}
	if len(*s.Passwd) > 0 {
		if len(*s.Username) == 0 {
			return errors.New("username is not set. -h or --help for more details")
		}
	}

	if len(*s.BootstrapServerPtr) == 0 {
		return errors.New("bootstrap server is not set. -h or --help for more details")
	}

	return nil
}

func (s Setting) Conf(gr string) (cli sarama.Client, err error) {
	config := sarama.NewConfig()
	if len(*s.Username) != 0 {
		config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &XDGSCRAMClient{HashGeneratorFcn: SHA256} }
		config.Net.SASL.Mechanism = sarama.SASLMechanism(sarama.SASLTypeSCRAMSHA256)
		config.Net.SASL.User = *s.Username
		config.Net.SASL.Password = *s.Passwd
		config.Net.SASL.Enable = true
	}
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Version = s.KafkaApiVersionFormated
	config.Producer.Return.Errors = true
	config.Producer.Return.Successes = true
	config.Consumer.Group.InstanceId = gr
	cli, err = sarama.NewClient(s.BootstrapServer, config)
	// cli, err = sarama.NewConsumer(s.BootstrapServer, config)
	if err != nil {
		return nil, err
	}
	return cli, nil
}
