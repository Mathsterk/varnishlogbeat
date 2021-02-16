package beater

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/elastic/beats/libbeat/beat"
	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/libbeat/logp"
	"github.com/elastic/beats/libbeat/publisher"

	"github.com/phenomenes/vago"
	"github.com/phenomenes/varnishlogbeat/config"
)

// Varnishlogbeat implements the Beater interface.
type Varnishlogbeat struct {
	client  publisher.Client
	varnish *vago.Varnish
	config  *vago.Config
}

// New creates a new Varnishlogbeat.
func New(b *beat.Beat, c *common.Config) (beat.Beater, error) {
	cfg := config.DefaultConfig
	if err := c.Unpack(&cfg); err != nil {
		return nil, fmt.Errorf("Error reading config file: %v", err)
	}

	vb := Varnishlogbeat{
		config: &vago.Config{
			Path:    cfg.Path,
			Timeout: cfg.Timeout,
		},
	}

	return &vb, nil
}

// Run opens a Varnish Shared Memory file and publishes log events.
func (vb *Varnishlogbeat) Run(b *beat.Beat) error {
	var err error

	logp.Info("varnishlogbeat is running! Hit CTRL-C to stop it.")

	vb.varnish, err = vago.Open(vb.config)
	if err != nil {
		return err
	}

	vb.client = b.Publisher.Connect()
	err = vb.harvest()
	if err != nil {
		logp.Err("%s", err)
	}

	return err
}

// harvest reads and parses Varnish log data.
func (vb *Varnishlogbeat) harvest() error {
	tx := make(common.MapStr)
	counter := 1
	vcllog := make(map[string]map[string][]interface{}, 0)
	vcldata := make(map[string][]interface{}, 0)

	vb.varnish.Log("",
		vago.REQ,
		vago.COPT_TAIL|vago.COPT_BATCH,
		func(vxid uint32, tag, _type, data string) int {
			switch _type {
			case "c":
				_type = "client"
			case "b":
				_type = "backend"
			default:
				return 0
			}

			switch tag {
			case "BereqHeader",
				"BerespHeader",
				"ObjHeader",
				"ReqHeader",
				"RespHeader":
				header := strings.SplitN(data, ": ", 2)
				key := strings.ToLower(header[0])
				var value interface{}
				switch {
				case key == "Content-Length":
					value, _ = strconv.Atoi(header[1])
				case len(header) == 2:
					value = header[1]
				// if the header is too long, header and value might get truncated
				default:
					value = "truncated"
				}
				if _, ok := tx[tag]; ok {
					tx[tag].(common.MapStr)[key] = value
				} else {
					tx[tag] = common.MapStr{key: value}
				}
			case "Timestamp":
				header := strings.SplitN(data, ": ", 2)
				key := header[0]
				var value interface{}
				switch {
				case key == "Content-Length":
					value, _ = strconv.Atoi(header[1])
				case len(header) == 2:
					value = header[1]
				// if the header is too long, header and value might get truncated
				default:
					value = "truncated"
				}
				if _, ok := tx[tag]; ok {
					tx[tag].(common.MapStr)[key] = value
				} else {
					tx[tag] = common.MapStr{key: value}
				}
			case "Length":
				tx[tag], _ = strconv.Atoi(data)

			case "End":
				event := common.MapStr{
					"@timestamp": common.Time(time.Now()),
					"count":      counter,
					"type":       _type,
					"vxid":       vxid,
					"tx":         tx,
					"VCL_Log":    vcllog,
					"VCL_data":   vcldata,
				}
				vb.client.PublishEvent(event)
				counter++
				logp.Info("Event sent")

				// destroy and re-create the map
				tx = nil
				tx = make(common.MapStr)

				vcllog = nil
				vcllog = make(map[string]map[string][]interface{}, 0)
				vcldata = nil
				vcldata = make(map[string][]interface{}, 0)

			case "VCL_Log":
				header := strings.SplitN(data, ":", 2)
				var value interface{}
				level, key, value := "UNKNOWN", "null", "null"
				switch {
				case len(header) == 2:
					split := strings.SplitN(header[0], "_", 2)
					switch {
					case len(split) == 2:
						level = strings.TrimSpace(split[0])
						key = strings.TrimSpace(split[1])
						value = strings.TrimSpace(header[1])
					default:
						key = strings.TrimSpace(header[0])
						value = strings.TrimSpace(header[1])
					}
				// if the header is too long, header and value might get truncated
				default:
					key = strings.TrimSpace(header[0])
					value = "null"
				}

				if _, ok := vcllog[level]; ok {
					if _, oki := vcllog[level][key]; oki {
						vcllog[level][key] = append(vcllog[level][key], value)
					} else {
						vcllog[level][key] = make([]interface{}, 0)
						vcllog[level][key] = append(vcllog[level][key], value)
					}
				} else {
					vcllog[level] = make(map[string][]interface{})
					vcllog[level][key] = make([]interface{}, 0)
					vcllog[level][key] = append(vcllog[level][key], value)
				}

			case "VCL_call",
				"VCL_return",
				"VCL_use":
				// vcldata := map[string][]interface{}

				var value interface{}
				value = data

				if _, ok := vcldata[tag]; ok {
					// vcllog[key] = append(vcllog[key], value)
				} else {
					vcldata[tag] = make([]interface{}, 0)
					vcldata[tag] = append(vcldata[tag], value)
				}

				// if _, ok := tx[tag]; ok {
				// 	tx[tag].(common.MapStr)[key] = value
				// 	// fmt.Printf("%d %s %s\n", txcounter[string(key)], key, value)
				// } else {
				// 	tx[tag] = common.MapStr{key: value}
				// 	// fmt.Printf("%d %s %s\n", txcounter[string(key)], key, value)
				// }
			default:
				tx[tag] = data
			}

			return 0
		})

	return nil
}

// Stop stops processing Varnish events, closes the VSM and publisher client.
func (vb *Varnishlogbeat) Stop() {
	vb.varnish.Stop()
	vb.varnish.Close()
	vb.client.Close()
}
