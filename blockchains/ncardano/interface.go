package ncardano

import (
	"diablo-benchmark/core"
	"fmt"
	"os"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

type BlockchainInterface struct {
}

func (this *BlockchainInterface) Builder(params map[string]string, env []string, endpoints map[string][]string, logger core.Logger) (core.BlockchainBuilder, error) {
	var key, value, endpoint string
	var builder *BlockchainBuilder
	var values []string

	logger.Debugf("new builder")

	envmap, err := parseEnvmap(env)
	if err != nil {
		return nil, err
	}

	for key = range endpoints {
		endpoint = key
		break
	}

	logger.Debugf("use endpoint '%s'", endpoint)
	builder = newBuilder(logger)

	for key, values = range envmap {
		if key == "accounts" {
			for _, value = range values {
				logger.Debugf("with accounts from '%s'", value)
				// TODO: Implement account loading
			}
			continue
		}

		if key == "contracts" {
			for _, value = range values {
				logger.Debugf("with contracts from '%s'", value)
				// TODO: Implement contract loading
			}
			continue
		}

		if key == "tx_prepare_config" {
			for _, value = range values {
				logger.Debugf("with transaction preparation config from '%s'", value)
				err := builder.loadTxPrepareConfig(value)
				if err != nil {
					return nil, fmt.Errorf("failed to load transaction preparation config from '%s': %w", value, err)
				}
			}
			continue
		}

		return nil, fmt.Errorf("unknown environment key '%s'", key)
	}

	return builder, nil
}

func (i *BlockchainInterface) Client(params map[string]string, env, view []string, logger core.Logger) (core.BlockchainClient, error) {
	logger.Tracef("new client")

	// For Cardano, we expect the first view to be the socket path
	if len(view) == 0 {
		return nil, fmt.Errorf("no socket path provided")
	}

	socketPath := view[0]
	logger.Tracef("using socket path: %s", socketPath)

	// Create the Cardano client
	client, err := NewBlockchainClient(logger, socketPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create Cardano client: %w", err)
	}

	// Handle any additional parameters
	for key, value := range params {
		if key == "prepare" {
			logger.Tracef("use prepare method '%s'", value)
			// TODO: Implement prepare method handling if needed
			continue
		}
		return nil, fmt.Errorf("unknown parameter '%s'", key)
	}

	return client, nil
}

func parseEnvmap(env []string) (map[string][]string, error) {
	var ret map[string][]string = make(map[string][]string)
	var element, key, value string
	var values []string
	var eqindex int
	var found bool

	for _, element = range env {
		eqindex = strings.Index(element, "=")
		if eqindex < 0 {
			return nil, fmt.Errorf("unexpected environment '%s'",
				element)
		}

		key = element[:eqindex]
		value = element[eqindex+1:]

		values, found = ret[key]
		if !found {
			values = make([]string, 0)
		}

		values = append(values, value)

		ret[key] = values
	}

	return ret, nil
}

// TxPrepareConfig represents transaction preparation configuration
type TxPrepareConfig struct {
	Splits         int    `yaml:"splits"`         // Total outputs to produce
	CapPerTx       int    `yaml:"cap_per_tx"`     // Max outputs per transaction
	Threads        int    `yaml:"threads"`        // Parallel workers per level
	InputAddrFile  string `yaml:"input_addr_file"`  // Input address file
	OutputAddrFile string `yaml:"output_addr_file"` // Output address file
	SkeyFile       string `yaml:"skey_file"`      // Signing key file
	FinalSkeyFile  string `yaml:"final_skey_file,omitempty"` // Final signing key file (optional)
	SkipPrepare    bool   `yaml:"skip_prepare"`   // Skip building final self-spend transactions
}
