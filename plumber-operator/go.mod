module github.com/VerstraeteBert/plumber-operator

go 1.15

require (
	github.com/go-critic/go-critic v0.5.6 // indirect
	github.com/go-logr/logr v0.4.0
	github.com/go-toolsmith/pkgload v1.0.1 // indirect
	github.com/kedacore/keda/v2 v2.2.0
	github.com/logrusorgru/aurora v2.0.3+incompatible // indirect
	github.com/onsi/ginkgo v1.15.2
	github.com/onsi/gomega v1.11.0
	github.com/quasilyte/go-ruleguard v0.3.5 // indirect
	github.com/quasilyte/regex/syntax v0.0.0-20200805063351-8f842688393c // indirect
	golang.org/x/mod v0.4.2 // indirect
	golang.org/x/sys v0.0.0-20210511113859-b0526f3d8744 // indirect
	honnef.co/go/tools v0.1.4 // indirect
	k8s.io/api v0.20.5
	k8s.io/apimachinery v0.20.5
	k8s.io/client-go v11.0.1-0.20190805182717-6502b5e7b1b5+incompatible
	sigs.k8s.io/controller-runtime v0.7.2
)

replace k8s.io/client-go => k8s.io/client-go v0.20.5
