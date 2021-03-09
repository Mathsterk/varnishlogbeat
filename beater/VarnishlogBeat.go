package beater

import (
	"errors"
	"fmt"
	"time"

	"github.com/elastic/beats/v7/libbeat/beat"
	"github.com/elastic/beats/v7/libbeat/common"
	"github.com/elastic/beats/v7/libbeat/logp"
	"github.com/phenomenes/vago"

	"github.com/mathsterk/varnishlogbeat/config"
)

// varnishlogbeat configuration.
type varnishlogbeat struct {
	done       chan struct{}
	config     config.Config
	client     beat.Client
	varnish    *vago.Varnish
	vagoconfig *vago.Config
}

// New creates an instance of varnishlogbeat.
func New(b *beat.Beat, cfg *common.Config) (beat.Beater, error) {
	c := config.DefaultConfig
	if err := cfg.Unpack(&c); err != nil {
		return nil, fmt.Errorf("Error reading config file: %v", err)
	}

	bt := &varnishlogbeat{
		done:   make(chan struct{}),
		config: c,
		vagoconfig: &vago.Config{
			Path:        c.Path,
			Timeout:     c.Timeout,
			VslReattach: 1,
		},
	}
	return bt, nil
}

// Run starts varnishlogbeat.
func (bt *varnishlogbeat) Run(b *beat.Beat) error {
	logp.Info("varnishlogbeat is running! Hit CTRL-C to stop it.")

	var err error
	bt.client, err = b.Publisher.Connect()
	bt.varnish, err = vago.Open(bt.vagoconfig)
	if err != nil {
		return err
	}

	err = bt.harvest()
	if err != nil {
		logp.Err("%s", err)
		return err
	}
	// ticker := time.NewTicker(bt.config.Period)
	// counter := 1
	// for {
	// 	select {
	// 	case <-bt.done:
	// 		return nil
	// 	case <-ticker.C:
	// 	}

	// 	event := beat.Event{
	// 		Timestamp: time.Now(),
	// 		Fields: common.MapStr{
	// 			"type":    b.Info.Name,
	// 			"counter": counter,
	// 		},
	// 	}
	// 	bt.client.Publish(event)
	// 	logp.Info("Event sent")
	// 	counter++
	// }
	return nil
}

// Stop stops varnishlogbeat.
func (bt *varnishlogbeat) Stop() {
	bt.client.Close()
	close(bt.done)
}

func (bt *varnishlogbeat) harvest() error {
	tx := make(common.MapStr)
	counter := 1
	// vcllog := make(map[string]map[string][]interface{}, 0)
	// vcldata := make(map[string][]interface{}, 0)
	halt := false
	vagoerror := errors.New("")
	for !halt {
		vagoerror := bt.varnish.Log("",
			vago.VXID,
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
				// case "BereqHeader",
				// 	"BerespHeader",
				// 	"ObjHeader",
				// 	"ReqHeader",
				// 	"RespHeader":
				// 	header := strings.SplitN(data, ": ", 2)
				// 	key := strings.ToLower(header[0])
				// 	var value interface{}
				// 	switch {
				// 	case key == "Content-Length":
				// 		value, _ = strconv.Atoi(header[1])
				// 	case len(header) == 2:
				// 		value = header[1]
				// 	// if the header is too long, header and value might get truncated
				// 	default:
				// 		value = "truncated"
				// 	}
				// 	if _, ok := tx[tag]; ok {
				// 		tx[tag].(common.MapStr)[key] = value
				// 	} else {
				// 		tx[tag] = common.MapStr{key: value}
				// 	}
				// case "Timestamp":
				// 	header := strings.SplitN(data, ": ", 2)
				// 	key := header[0]
				// 	var value interface{}
				// 	switch {
				// 	case key == "Content-Length":
				// 		value, _ = strconv.Atoi(header[1])
				// 	case len(header) == 2:
				// 		value = header[1]
				// 	// if the header is too long, header and value might get truncated
				// 	default:
				// 		value = "truncated"
				// 	}
				// 	if _, ok := tx[tag]; ok {
				// 		tx[tag].(common.MapStr)[key] = value
				// 	} else {
				// 		tx[tag] = common.MapStr{key: value}
				// 	}
				// case "Length":
				// 	tx[tag], _ = strconv.Atoi(data)

				case "End":
					// event := common.MapStr{
					// 	"@timestamp": common.Time(time.Now()),
					// 	"count":      counter,
					// 	"type":       _type,
					// 	"vxid":       vxid,
					// 	"tx":         tx,
					// 	"VCL_Log":    vcllog,
					// 	"VCL_data":   vcldata,
					// }
					// bt.client.Publish(event)

					event := beat.Event{
						Timestamp: time.Now(),
						Fields: common.MapStr{
							// "type":    b.Info.Name,
							"count": counter,
							"type":  _type,
							"vxid":  vxid,
							"tx":    tx,
							// "VCL_Log":  vcllog,
							// "VCL_data": vcldata,
						},
					}
					bt.client.Publish(event)

					counter++
					logp.Info("Event sent")

					// destroy and re-create the map
					tx = nil
					tx = make(common.MapStr)

					// vcllog = nil
					// vcllog = make(map[string]map[string][]interface{}, 0)
					// vcldata = nil
					// vcldata = make(map[string][]interface{}, 0)

					// case "VCL_Log":
					// 	header := strings.SplitN(data, ":", 2)
					// 	var value interface{}
					// 	level, key, value := "UNKNOWN", "null", "null"
					// 	switch {
					// 	case len(header) == 2:
					// 		split := strings.SplitN(header[0], "_", 2)
					// 		switch {
					// 		case len(split) == 2:
					// 			level = strings.TrimSpace(split[0])
					// 			key = strings.TrimSpace(split[1])
					// 			value = strings.TrimSpace(header[1])
					// 		default:
					// 			key = strings.TrimSpace(header[0])
					// 			value = strings.TrimSpace(header[1])
					// 		}
					// 	// if the header is too long, header and value might get truncated
					// 	default:
					// 		key = strings.TrimSpace(header[0])
					// 		value = "null"
					// 	}

					// 	if _, ok := vcllog[level]; ok {
					// 		if _, oki := vcllog[level][key]; oki {
					// 			vcllog[level][key] = append(vcllog[level][key], value)
					// 		} else {
					// 			vcllog[level][key] = make([]interface{}, 0)
					// 			vcllog[level][key] = append(vcllog[level][key], value)
					// 		}
					// 	} else {
					// 		vcllog[level] = make(map[string][]interface{})
					// 		vcllog[level][key] = make([]interface{}, 0)
					// 		vcllog[level][key] = append(vcllog[level][key], value)
					// 	}

					// case "VCL_call",
					// 	"VCL_return",
					// 	"VCL_use":
					// 	// vcldata := map[string][]interface{}

					// 	var value interface{}
					// 	value = data

					// 	if _, ok := vcldata[tag]; ok {
					// 		vcldata[tag] = append(vcldata[tag], value)
					// 	} else {
					// 		vcldata[tag] = make([]interface{}, 0)
					// 		vcldata[tag] = append(vcldata[tag], value)
					// 	}

					// 	// if _, ok := tx[tag]; ok {
					// 	// 	tx[tag].(common.MapStr)[key] = value
					// 	// 	// fmt.Printf("%d %s %s\n", txcounter[string(key)], key, value)
					// 	// } else {
					// 	// 	tx[tag] = common.MapStr{key: value}
					// 	// 	// fmt.Printf("%d %s %s\n", txcounter[string(key)], key, value)
					// 	// }
					// default:
					// 	tx[tag] = data
				}

				return 0
			})
		if vagoerror != vago.ErrOverrun {
			halt = true
		} else {
			logp.Warn("Log Overrun!!!")
		}
		// fmt.Printf("%q", vagoerror)
		// return vagoerror
		// logp.Error(vagoerror)
	}
	return vagoerror
	// return nil
}
