package main

import "testing"

func TestLocalStarGiftWithdrawalRequiresPublicListener(t *testing.T) {
	option, err := localStarGiftWithdrawalOption("not-a-url", "")
	if err != nil || option != nil {
		t.Fatalf("disabled public listener option=%v err=%v, want nil without URL construction", option != nil, err)
	}
	option, err = localStarGiftWithdrawalOption("https://links.example.test", "127.0.0.1:2401")
	if err != nil || option == nil {
		t.Fatalf("enabled public listener option=%v err=%v", option != nil, err)
	}
	if option, err = localStarGiftWithdrawalOption("not-a-url", "127.0.0.1:2401"); err == nil || option != nil {
		t.Fatalf("enabled listener accepted dead public URL: option=%v err=%v", option != nil, err)
	}
}
