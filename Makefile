.PHONY: all clean install build
all: build doc

export OCAMLRUNPARAM=b
NAME=xenvmidl

BINDIR?=/sbin

setup.bin: setup.ml
	@ocamlopt.opt -o $@ $< || ocamlopt -o $@ $< || ocamlc -o $@ $<
	@rm -f setup.cmx setup.cmi setup.o setup.cmo

setup.data: setup.bin
	@./setup.bin -configure --enable-tests

build: setup.data setup.bin
	@./setup.bin -build

doc: setup.data setup.bin
	@./setup.bin -doc

setup.ml: _oasis
	@oasis setup

install: setup.bin
	install -m 0755 xenvmd.native ${BINDIR}/xenvmd
	install -m 0755 xenvm.native ${BINDIR}/xenvm
	install -m 0755 local_allocator.native ${BINDIR}/xenvm-local-allocator

uninstall:
	@ocamlfind remove $(NAME) || true

test: setup.bin build
	@./setup.bin -test

reinstall: setup.bin
	@ocamlfind remove $(NAME) || true
	@./setup.bin -reinstall

clean:
	@ocamlbuild -clean
	@rm -f setup.data setup.log setup.bin
