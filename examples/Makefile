# -*- Mode: makefile-gmake -*-

.PHONY: clean all debug release

#
# Executable and source
#

EXE := example_echo
SRC := $(EXE).c

#
# Default target
#

all: debug release

#
# Required packages
#

PKGS = libglibutil glib-2.0 gio-2.0 gio-unix-2.0 gobject-2.0

LIBGIORPC_INSTALLED := $(shell if pkg-config --exists libgiorpc ; then echo YES ; else echo NO; fi)

ifeq ($(LIBGIORPC_INSTALLED),YES)

# libgiorpc development package seems to be installed
PKGS += libgiorpc

else

LIBGIORPC_PATH = ..
INCLUDES += -I$(LIBGIORPC_PATH)/include
DEBUG_LIBS += -L$(LIBGIORPC_PATH)/build/debug -lgiorpc
RELEASE_LIBS += -L$(LIBGIORPC_PATH)/build/release -lgiorpc
DEBUG_DEPS += libgiorpc_debug
RELEASE_DEPS += libgiorpc_release

.PHONY: libgiorpc_debug libgiorpc_release

libgiorpc_debug:
	make -C $(LIBGIORPC_PATH) debug

libgiorpc_release:
	make -C $(LIBGIORPC_PATH) release

endif

#
# Directories
#

SRC_DIR := .
BUILD_DIR := build
DEBUG_BUILD_DIR := $(BUILD_DIR)/debug
RELEASE_BUILD_DIR := $(BUILD_DIR)/release

#
# Tools and flags
#

CC ?= $(CROSS_COMPILE)gcc
LD = $(CC)
WARNINGS := -Wall
BASE_FLAGS := -fPIC
BASE_LDFLAGS := $(BASE_FLAGS) $(LDFLAGS)
BASE_CFLAGS := $(BASE_FLAGS) $(CFLAGS)
FULL_CFLAGS := $(BASE_CFLAGS) $(DEFINES) $(WARNINGS) $(INCLUDES) -MMD -MP \
  $(shell pkg-config --cflags $(PKGS))
FULL_LDFLAGS := $(BASE_LDFLAGS)
DEBUG_FLAGS := -g
RELEASE_FLAGS =

LIBS := $(shell pkg-config --libs $(PKGS))
DEBUG_LIBS += $(LIBS)
RELEASE_LIBS += $(LIBS)

DEBUG_LDFLAGS := $(FULL_LDFLAGS) $(DEBUG_FLAGS)
RELEASE_LDFLAGS := $(FULL_LDFLAGS) $(RELEASE_FLAGS)
DEBUG_CFLAGS := $(FULL_CFLAGS) $(DEBUG_FLAGS) -DDEBUG
RELEASE_CFLAGS := $(FULL_CFLAGS) $(RELEASE_FLAGS) -O2

#
# Files
#

DEBUG_OBJS := $(SRC:%.c=$(DEBUG_BUILD_DIR)/%.o)
RELEASE_OBJS := $(SRC:%.c=$(RELEASE_BUILD_DIR)/%.o)

DEBUG_EXE := $(DEBUG_BUILD_DIR)/$(EXE)
RELEASE_EXE := $(RELEASE_BUILD_DIR)/$(EXE)

#
# Dependencies
#

DEPS := $(DEBUG_OBJS:%.o=%.d) $(RELEASE_OBJS:%.o=%.d)
ifneq ($(MAKECMDGOALS),clean)
ifneq ($(strip $(DEPS)),)
-include $(DEPS)
endif
endif

$(DEBUG_OBJS): | $(DEBUG_BUILD_DIR) $(DEBUG_DEPS)
$(RELEASE_OBJS): | $(RELEASE_BUILD_DIR) $(RELEASE_DEPS)

#
# Rules
#

debug: $(DEBUG_EXE)

release: $(RELEASE_EXE)

clean:
	rm -f *~
	rm -fr $(BUILD_DIR)

$(DEBUG_BUILD_DIR):
	mkdir -p $@

$(RELEASE_BUILD_DIR):
	mkdir -p $@

$(DEBUG_BUILD_DIR)/%.o : $(SRC_DIR)/%.c
	$(CC) -c $(DEBUG_CFLAGS) -MT"$@" -MF"$(@:%.o=%.d)" $< -o $@

$(RELEASE_BUILD_DIR)/%.o : $(SRC_DIR)/%.c
	$(CC) -c $(RELEASE_CFLAGS) -MT"$@" -MF"$(@:%.o=%.d)" $< -o $@

$(DEBUG_EXE): $(DEBUG_OBJS)
	$(LD) $(DEBUG_LDFLAGS) $(DEBUG_OBJS) $(DEBUG_LIBS) -o $@

$(RELEASE_EXE): $(RELEASE_OBJS)
	$(LD) $(RELEASE_LDFLAGS) $(RELEASE_OBJS) $(RELEASE_LIBS) -o $@
ifeq ($(KEEP_SYMBOLS),0)
	strip $@
endif
