package contract_test

import (
	"errors"
	"testing"
	"time"

	"fintech-ledger-playground/internal/moneyout/contract"
)

func validRequest() contract.TransferRequest {
	return contract.TransferRequest{RequestID: "01a1d000-0000-7000-8000-0000000000aa", TransferID: "01a1d000-0000-7000-8000-000000000001",
		AttemptNo: 1, Rail: contract.RailBIFAST, Amount: contract.Amount{Value: "2500000.00", Currency: "IDR"},
		Beneficiary: contract.Beneficiary{BankCode: "014", AccountNo: "0201245681"}, IssuedAt: time.Now(), Nonce: "bm9uY2U"}
}

func TestTransferRequestValidate(t *testing.T) {
	if err := validRequest().Validate(); err != nil {
		t.Fatal(err)
	}
	for name, mut := range map[string]func(*contract.TransferRequest){
		"request_id":  func(r *contract.TransferRequest) { r.RequestID = "x" },
		"transfer_id": func(r *contract.TransferRequest) { r.TransferID = "01A1D000-0000-7000-8000-000000000001" },
		"attempt_no":  func(r *contract.TransferRequest) { r.AttemptNo = 0 },
		"rail":        func(r *contract.TransferRequest) { r.Rail = "SWIFT" },
		"rail case":   func(r *contract.TransferRequest) { r.Rail = "bifast" },
		"amount":      func(r *contract.TransferRequest) { r.Amount.Value = "2500000.50" },
		"currency":    func(r *contract.TransferRequest) { r.Amount.Currency = "USD" },
		"bank_code":   func(r *contract.TransferRequest) { r.Beneficiary.BankCode = "14" },
		"account_no":  func(r *contract.TransferRequest) { r.Beneficiary.AccountNo = "02-01" },
		"issued_at":   func(r *contract.TransferRequest) { r.IssuedAt = time.Time{} },
		"nonce":       func(r *contract.TransferRequest) { r.Nonce = "" },
	} {
		r := validRequest()
		mut(&r)
		if err := r.Validate(); !errors.Is(err, contract.ErrInvalidRequest) {
			t.Errorf("%s: %v, want ErrInvalidRequest", name, err)
		}
	}
}

func TestStatusInquiryValidate(t *testing.T) {
	ok := contract.StatusInquiry{TransferID: "01a1d000-0000-7000-8000-000000000001", RequestID: "01a1d000-0000-7000-8000-0000000000aa"}
	if err := ok.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []contract.StatusInquiry{{TransferID: ok.TransferID}, {RequestID: ok.RequestID}, {TransferID: "x", RequestID: "y"}} {
		if err := bad.Validate(); !errors.Is(err, contract.ErrInvalidRequest) {
			t.Errorf("%+v: %v", bad, err)
		}
	}
}

func TestRails(t *testing.T) {
	for _, r := range []string{"BIFAST", "RTOL", "SKNBI", "RTGS", "INTRABANK"} {
		if !contract.ValidRail(r) {
			t.Errorf("%s not valid", r)
		}
	}
	if contract.ValidRail("RTO") {
		t.Error("RTO is spelled RTOL (Q5)")
	}
}
