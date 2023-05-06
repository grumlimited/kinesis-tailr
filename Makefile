# Install to /usr unless otherwise specified, such as `make PREFIX=/app`
PREFIX=/usr

NAME=kinesis-tailf

# What to run to install various files
INSTALL=install
# Run to install the actual binary
INSTALL_PROGRAM=$(INSTALL)

# Directories into which to install the various files
bindir=$(DESTDIR)$(PREFIX)/bin
sharedir=$(DESTDIR)$(PREFIX)/share

# These targets have no associated build files.
.PHONY : clean clean-all install uninstall

# Build the application
release : src
	cargo build --release

run :
	cargo run

clippy :
	find src/ -name "*.rs" -exec touch {} \;
	cargo clippy

install : release
	mkdir -p $(bindir)

	sudo $(INSTALL_PROGRAM) -m 0755 target/release/$(NAME) $(bindir)/$(NAME)

# Remove all files
clean-all : clean
	cargo clean

# Remove supplemental build files
clean :
	rm -rf target/*

uninstall :
	rm -f $(bindir)/$(NAME)