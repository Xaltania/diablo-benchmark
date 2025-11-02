package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"reflect"
	"strconv"
	"strings"

	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/protocol/localstatequery"
)

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: %s -socket <unix-path|host:port> -magic <network-magic>\n", os.Args[0])
		flag.PrintDefaults()
	}
	socket := flag.String("socket", "", "cardano-node socket path (NtC) or host:port (via socat/TCP)")
	magicStr := flag.String("magic", "", "network magic (e.g. 764824073 for mainnet, 1 for preprod)")
	flag.Parse()

	if *socket == "" || *magicStr == "" {
		flag.Usage()
		os.Exit(2)
	}
	magicU64, err := strconv.ParseUint(*magicStr, 10, 32)
	if err != nil {
		log.Fatalf("invalid magic: %v", err)
	}

	// Build the Ouroboros connection
	conn, err := ouroboros.NewConnection(
		ouroboros.WithNetworkMagic(uint32(magicU64)),
		ouroboros.WithNodeToNode(false), // node-to-client
		ouroboros.WithKeepAlive(true),
		ouroboros.WithLocalStateQueryConfig(localstatequery.NewConfig()),
	)
	if err != nil {
		log.Fatalf("new connection: %v", err)
	}
	defer conn.Close()

	// Dial the node (unix for local socket, tcp if host:port)
	proto := "unix"
	if strings.Contains(*socket, ":") {
		proto = "tcp"
	}
	if err := conn.Dial(proto, *socket); err != nil {
		log.Fatalf("dial %s %s: %v", proto, *socket, err)
	}

	// Fetch protocol parameters
	pp, err := conn.LocalStateQuery().Client.GetGenesisConfig()
	if err != nil {
		log.Fatalf("query protocol params: %v", err)
	}

	// Use reflection to access struct fields
	v := reflect.ValueOf(pp)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	// Extract and print the requested values
	fmt.Println("Protocol Parameters:")
	fmt.Println("===================")

	// Helper function to safely get and print field values
	printField := func(fieldName string) {
		field := v.FieldByName(fieldName)
		if !field.IsValid() {
			fmt.Printf("%s: (not found)\n", fieldName)
			return
		}

		// Handle ActiveSlotsCoeff which appears to be a slice representing a rational number
		if fieldName == "ActiveSlotsCoeff" && field.Kind() == reflect.Slice {
			if field.Len() == 2 {
				num := field.Index(0).Interface()
				den := field.Index(1).Interface()
				// Try to convert to float for display
				if numFloat, ok := convertToFloat(num); ok {
					if denFloat, ok := convertToFloat(den); ok && denFloat != 0 {
						result := numFloat / denFloat
						fmt.Printf("%s: %v (%.6f)\n", fieldName, field.Interface(), result)
						return
					}
				}
				fmt.Printf("%s: %v\n", fieldName, field.Interface())
				return
			}
		}

		// Handle SlotLength which is likely in microseconds
		if fieldName == "SlotLength" {
			fmt.Printf("%s: %v", fieldName, field.Interface())
			if field.Kind() == reflect.Uint64 || field.Kind() == reflect.Int64 || field.Kind() == reflect.Int {
				// Assume it's in microseconds, convert to seconds
				var microsec int64
				switch field.Kind() {
				case reflect.Uint64:
					microsec = int64(field.Uint())
				case reflect.Int64, reflect.Int:
					microsec = field.Int()
				}
				fmt.Printf(" (%d microseconds = %.6f seconds)", microsec, float64(microsec)/1000000.0)
			}
			fmt.Println()
			return
		}

		fmt.Printf("%s: %v\n", fieldName, field.Interface())
	}

	printField("ActiveSlotsCoeff")
	printField("SecurityParam")
	printField("EpochLength")
	printField("SlotLength")
	printField("SlotsPerKESPeriod")
}

// Helper function to convert various numeric types to float64
func convertToFloat(v interface{}) (float64, bool) {
	switch val := v.(type) {
	case float64:
		return val, true
	case float32:
		return float64(val), true
	case int:
		return float64(val), true
	case int8:
		return float64(val), true
	case int16:
		return float64(val), true
	case int32:
		return float64(val), true
	case int64:
		return float64(val), true
	case uint:
		return float64(val), true
	case uint8:
		return float64(val), true
	case uint16:
		return float64(val), true
	case uint32:
		return float64(val), true
	case uint64:
		return float64(val), true
	default:
		return 0, false
	}
}
