package chaincode

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"

	"github.com/hyperledger/fabric-contract-api-go/v2/contractapi"
)

// SmartContract provides functions for managing an Asset
type SmartContract struct {
	contractapi.Contract
}

// TripData struct para representar os dados de uma viagem
type TripData struct {
	DepartureDatetime string  json:"Departure_Datetime"
	TotalDistanceKm   float64 json:"totalDistance_km"
	ID                int     json:"TripID"
	ArrivalDatetime   string  json:"Arrival_Datetime"
}

// InitLedger adiciona um conjunto básico de ativos ao ledger
func (s *SmartContract) InitLedger(ctx contractapi.TransactionContextInterface) error {
	assets := []TripData{
		{ID: 0, DepartureDatetime: "00", TotalDistanceKm: 5, ArrivalDatetime: "00"},
		{ID: 1, DepartureDatetime: "11", TotalDistanceKm: 5, ArrivalDatetime: "00"},
		{ID: 2, DepartureDatetime: "2", TotalDistanceKm: 10, ArrivalDatetime: "00"},
		{ID: 3, DepartureDatetime: "1", TotalDistanceKm: 10, ArrivalDatetime: "00"},
		{ID: 4, DepartureDatetime: "15", TotalDistanceKm: 15, ArrivalDatetime: "00"},
		{ID: 5, DepartureDatetime: "12", TotalDistanceKm: 15, ArrivalDatetime: "00"},
	}

	for _, asset := range assets {
		assetJSON, err := json.Marshal(asset)
		if err != nil {
			return err
		}

		err = ctx.GetStub().PutState(fmt.Sprintf("%d", asset.ID), assetJSON)
		if err != nil {
			return fmt.Errorf("falha ao adicionar ao estado mundial: %v", err)
		}
	}
	fmt.Println("Inicializando o ledger")
	return nil
}

// CreateAsset emite um novo ativo para o estado mundial com os detalhes fornecidos.
func (s *SmartContract) CreateAsset(ctx contractapi.TransactionContextInterface, id int, departureDatetime string, arrivalDatetime string, totalDistanceKm float64) error {
	exists, err := s.AssetExists(ctx, fmt.Sprintf("%d", id))
	if err != nil {
		return err
	}
	if exists {
		return fmt.Errorf("o ativo %d já existe", id)
	}

	asset := TripData{
		ID:                id,
		DepartureDatetime: departureDatetime,
		ArrivalDatetime:   arrivalDatetime,
		TotalDistanceKm:   totalDistanceKm,
	}
	assetJSON, err := json.Marshal(asset)
	if err != nil {
		return err
	}

	return ctx.GetStub().PutState(fmt.Sprintf("%d", id), assetJSON)
}


// ReadAsset retorna o ativo armazenado no estado mundial com o ID fornecido.
func (s *SmartContract) ReadAsset(ctx contractapi.TransactionContextInterface, id string) (*TripData, error) {
	assetJSON, err := ctx.GetStub().GetState(id)
	if err != nil {
		return nil, fmt.Errorf("falha ao ler do estado mundial: %v", err)
	}
	if assetJSON == nil {
		return nil, fmt.Errorf("o ativo %s não existe", id)
	}

	var asset TripData
	err = json.Unmarshal(assetJSON, &asset)
	if err != nil {
		return nil, err
	}

	return &asset, nil
}


// AssetExists retorna verdadeiro quando o ativo com o ID fornecido existe no estado mundial.
func (s *SmartContract) AssetExists(ctx contractapi.TransactionContextInterface, id string) (bool, error) {
	assetJSON, err := ctx.GetStub().GetState(id)
	if err != nil {
		return false, fmt.Errorf("falha ao ler do estado mundial: %v", err)
	}

	return assetJSON != nil, nil
}


// GetAllAssets retorna todos os ativos encontrados no estado mundial.
func (s *SmartContract) GetAllAssets(ctx contractapi.TransactionContextInterface) ([]*TripData, error) {
	resultsIterator, err := ctx.GetStub().GetStateByRange("", "")
	if err != nil {
		return nil, err
	}
	defer resultsIterator.Close()

	var assets []*TripData
	for resultsIterator.HasNext() {
		queryResponse, err := resultsIterator.Next()
		if err != nil {
			return nil, err
		}

		var asset TripData
		err = json.Unmarshal(queryResponse.Value, &asset)
		if err != nil {
			return nil, err
		}
		assets = append(assets, &asset)
	}

	return assets, nil
}


// QueryBanco function to query data from MySQL and add transactions to the ledger
func (mc *SmartContract) QueryBanco(ctx contractapi.TransactionContextInterface) ([]byte, error) {
	// Conexão com o MySQL
	db, err := sql.Open("mysql", "root:movepass@tcp(localhost:3306)/moveuff")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Query SQL
	queryToday := `
	SELECT 
		departure.id AS Departure_Datetime, 
		trips.totalDistance_km, 
		trips.id AS TripID, 
		arrival.id AS Arrival_Datetime
	FROM trip_x_parkingslot_departures AS departure
	JOIN trips ON departure.Trips_id = trips.id
	JOIN trip_x_parkingslot_arrivals AS arrival ON arrival.Trips_id = trips.id
	WHERE DATE(departure.id) = CURDATE() AND DATE(arrival.id) = CURDATE()
`

	// Executar a query
	rows, err := db.Query(queryToday)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	// Variável para armazenar o array JSON
	var resultArray []map[string]interface{}

	// Variável para armazenar a soma de totalDistance_km
	totalDistanceSum := 0.0

	for rows.Next() {
		var departureDatetime string
		var totalDistanceKm float64
		var tripID int
		var arrivalDatetime string

		// Ler os valores do resultado da query
		err := rows.Scan(&departureDatetime, &totalDistanceKm, &tripID, &arrivalDatetime)
		if err != nil {
			log.Fatal(err)
		}

		// Criar um mapa com os dados da linha atual
		rowData := map[string]interface{}{
			"Departure_Datetime": departureDatetime,
			"totalDistance_km":   totalDistanceKm,
			"TripID":             tripID,
			"Arrival_Datetime":   arrivalDatetime,
		}
		CreateAsset(tripID, departureDatetime, arrivalDatetime, totalDistanceKm)
		// Somar o valor de totalDistance_km
		totalDistanceSum += totalDistanceKm
	}
}