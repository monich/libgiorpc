/*
 * Copyright (C) 2023-2025 Slava Monich <slava@monich.com>
 *
 * You may use this file under the terms of the BSD license as follows:
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 *  1. Redistributions of source code must retain the above copyright
 *     notice, this list of conditions and the following disclaimer.
 *
 *  2. Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer
 *     in the documentation and/or other materials provided with the
 *     distribution.
 *
 *  3. Neither the names of the copyright holders nor the names of its
 *     contributors may be used to endorse or promote products derived
 *     from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * HOLDERS OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * The views and conclusions contained in the software and documentation
 * are those of the authors and should not be interpreted as representing
 * any official policies, either expressed or implied.
 */

#ifndef GIORPC_VERSION_H
#define GIORPC_VERSION_H

/*
 * GIORPC_VERSION_X_Y_Z macros will be added with each release. The fact that
 * such macro is defined means that you're compiling against libgiorpc version
 * X.Y.Z or greater.
 *
 * The versions with the same X.Y part are perfectly compatible both ways,
 * i.e. they export the same set of symbols. Increment of Z (release) part
 * essentially marks an internal bug fix.
 *
 * The versions with different X.Y part are backward compatible.
 *
 * The actual version of the shared library can be queried at run time with
 * giorpc_version() and may differ from the version you compiled against.
 */

#define GIORPC_VERSION_MAJOR   1
#define GIORPC_VERSION_MINOR   0
#define GIORPC_VERSION_RELEASE 1
#define GIORPC_VERSION_STRING  "1.0.1"

/* Version as a single word */
#define GIORPC_VERSION_(v1,v2,v3) \
    ((((v1) & 0x7f) << 24) | \
     (((v2) & 0xfff) << 12) | \
      ((v3) & 0xfff))

#define GIORPC_VERSION_GET_MAJOR(v)   (((v) >> 24) & 0x7f)
#define GIORPC_VERSION_GET_MINOR(v)   (((v) >> 12) & 0xfff)
#define GIORPC_VERSION_GET_RELEASE(v) (((v) & 0xfff))

/* Current version as a single word */
#define GIORPC_VERSION GIORPC_VERSION_ \
    (GIORPC_VERSION_MAJOR, GIORPC_VERSION_MINOR, GIORPC_VERSION_RELEASE)

/* Specific versions */
#define GIORPC_VERSION_1_0_0 GIORPC_VERSION_(1,0,0)
#define GIORPC_VERSION_1_0_1 GIORPC_VERSION_(1,0,1)

#endif /* GIORPC_VERSION_H */

/*
 * Local Variables:
 * mode: C
 * c-basic-offset: 4
 * indent-tabs-mode: nil
 * End:
 */
