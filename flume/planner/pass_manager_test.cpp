/***************************************************************************
 *
 * Copyright (c) 2013 Baidu, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 **************************************************************************/
// Author: Wen Xiang <bigflow-opensource@baidu.com>

#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "flume/planner/pass.h"
#include "flume/planner/pass_manager.h"
#include "flume/planner/plan.h"

namespace baidu {
namespace flume {
namespace planner {

using ::testing::ElementsAre;
using ::testing::InSequence;
using ::testing::Return;

TEST(BasicTest, RegisterRelations) {
    PassManager::ShowPassRelations(&LOG(INFO));
}

class InvalidateByDefaultTest : public ::testing::Test {
public:
    class Pass1 : public Pass {
    public:
        MOCK_METHOD1(Run, bool (Plan*));  // NOLINT
    };

    class Pass2 : public Pass {
    public:
        MOCK_METHOD1(Run, bool (Plan*));  // NOLINT
    };
};

TEST_F(InvalidateByDefaultTest, Run) {
    Pass1 pass1;
    Pass2 pass2;

    {
        InSequence in_sequence;

        EXPECT_CALL(pass1, Run(NULL)).WillOnce(Return(true));
        EXPECT_CALL(pass2, Run(NULL)).WillOnce(Return(true));
        EXPECT_CALL(pass1, Run(NULL)).WillOnce(Return(false));
    }

    PassManager manager(NULL);
    manager.RegisterPass(&pass1);
    manager.RegisterPass(&pass2);
    EXPECT_EQ(false, manager.IsValid<Pass1>());
    EXPECT_EQ(false, manager.IsValid<Pass2>());

    EXPECT_EQ(true, manager.Apply<Pass1>());
    EXPECT_EQ(true, manager.IsValid<Pass1>());
    EXPECT_EQ(false, manager.IsValid<Pass2>());

    EXPECT_EQ(true, manager.Apply<Pass2>());
    EXPECT_EQ(false, manager.IsValid<Pass1>());
    EXPECT_EQ(true, manager.IsValid<Pass2>());

    EXPECT_EQ(false, manager.Apply<Pass1>());
    EXPECT_EQ(true, manager.IsValid<Pass1>());
    EXPECT_EQ(true, manager.IsValid<Pass2>());
}

class HoldByDefaultTest : public ::testing::Test {
public:
    class Pass1 : public Pass {
        HOLD_BY_DEFAULT();
    public:
        MOCK_METHOD1(Run, bool (Plan*));  // NOLINT
    };

    class Pass2 : public Pass {
    public:
        MOCK_METHOD1(Run, bool (Plan*));  // NOLINT
    };
};

TEST_F(HoldByDefaultTest, Run) {
    Pass1 pass1;
    Pass2 pass2;

    {
        InSequence in_sequence;

        EXPECT_CALL(pass2, Run(NULL)).WillOnce(Return(true));
        EXPECT_CALL(pass1, Run(NULL)).WillOnce(Return(true));
        EXPECT_CALL(pass2, Run(NULL)).WillOnce(Return(true));
    }

    PassManager manager(NULL);
    manager.RegisterPass(&pass1);
    manager.RegisterPass(&pass2);
    EXPECT_EQ(false, manager.IsValid<Pass1>());
    EXPECT_EQ(false, manager.IsValid<Pass2>());

    EXPECT_EQ(true, manager.Apply<Pass2>());
    EXPECT_EQ(false, manager.IsValid<Pass1>());
    EXPECT_EQ(true, manager.IsValid<Pass2>());

    EXPECT_EQ(true, manager.Apply<Pass1>());
    EXPECT_EQ(true, manager.IsValid<Pass1>());
    EXPECT_EQ(false, manager.IsValid<Pass2>());

    EXPECT_EQ(true, manager.Apply<Pass2>());
    EXPECT_EQ(true, manager.IsValid<Pass1>());
    EXPECT_EQ(true, manager.IsValid<Pass2>());
}

class PreserveByDefaultTest : public ::testing::Test {
public:
    class Pass1 : public Pass {
    public:
        MOCK_METHOD1(Run, bool (Plan*));  // NOLINT
    };

    class Pass2 : public Pass {
        PRESERVE_BY_DEFAULT();
    public:
        MOCK_METHOD1(Run, bool (Plan*));  // NOLINT
    };
};

TEST_F(PreserveByDefaultTest, Run) {
    Pass1 pass1;
    Pass2 pass2;

    {
        InSequence in_sequence;

        EXPECT_CALL(pass1, Run(NULL)).WillOnce(Return(true));
        EXPECT_CALL(pass2, Run(NULL)).WillOnce(Return(true));
    }

    PassManager manager(NULL);
    manager.RegisterPass(&pass1);
    manager.RegisterPass(&pass2);
    EXPECT_EQ(false, manager.IsValid<Pass1>());
    EXPECT_EQ(false, manager.IsValid<Pass2>());

    EXPECT_EQ(true, manager.Apply<Pass1>());
    EXPECT_EQ(true, manager.IsValid<Pass1>());
    EXPECT_EQ(false, manager.IsValid<Pass2>());

    EXPECT_EQ(true, manager.Apply<Pass2>());
    EXPECT_EQ(true, manager.IsValid<Pass1>());
    EXPECT_EQ(true, manager.IsValid<Pass2>());
}

class InvalidateTest : public ::testing::Test {
public:
    class Pass1A : public Pass {
        HOLD_BY_DEFAULT();
    public:
        MOCK_METHOD1(Run, bool (Plan*));  // NOLINT
    };

    class Pass1B : public Pass {
        RELY_PASS(Pass1A);
        INVALIDATE_PASS(Pass1A);
    public:
        MOCK_METHOD1(Run, bool (Plan*));  // NOLINT
    };

    class Pass2A : public Pass {
    public:
        MOCK_METHOD1(Run, bool (Plan*));  // NOLINT
    };

    class Pass2B : public Pass {
        RELY_PASS(Pass2A);
        PRESERVE_BY_DEFAULT();
        INVALIDATE_PASS(Pass2A);
    public:
        MOCK_METHOD1(Run, bool (Plan*));  // NOLINT
    };
};

TEST_F(InvalidateTest, HoldByDefault) {
    Pass1A pass_a;
    Pass1B pass_b;

    {
        InSequence in_sequence;

        EXPECT_CALL(pass_a, Run(NULL)).WillOnce(Return(true));
        EXPECT_CALL(pass_b, Run(NULL)).WillOnce(Return(true));
    }

    PassManager manager(NULL);
    manager.RegisterPass(&pass_a);
    manager.RegisterPass(&pass_b);
    EXPECT_EQ(false, manager.IsValid<Pass1A>());
    EXPECT_EQ(false, manager.IsValid<Pass1B>());

    EXPECT_EQ(true, manager.Apply<Pass1B>());
    EXPECT_EQ(false, manager.IsValid<Pass1A>());
    EXPECT_EQ(true, manager.IsValid<Pass1B>());
}

TEST_F(InvalidateTest, PreserveByDefault) {
    Pass2A pass_a;
    Pass2B pass_b;

    {
        InSequence in_sequence;

        EXPECT_CALL(pass_a, Run(NULL)).WillOnce(Return(true));
        EXPECT_CALL(pass_b, Run(NULL)).WillOnce(Return(true));
    }

    PassManager manager(NULL);
    manager.RegisterPass(&pass_a);
    manager.RegisterPass(&pass_b);
    EXPECT_EQ(false, manager.IsValid<Pass2A>());
    EXPECT_EQ(false, manager.IsValid<Pass2B>());

    EXPECT_EQ(true, manager.Apply<Pass2B>());
    EXPECT_EQ(false, manager.IsValid<Pass2A>());
    EXPECT_EQ(true, manager.IsValid<Pass2B>());
}

class PreserveTest : public ::testing::Test {
public:
    class Pass1 : public Pass {
        PRESERVE_BY_DEFAULT();
    public:
        MOCK_METHOD1(Run, bool (Plan*));  // NOLINT
    };

    class Pass2 : public Pass {
        RELY_PASS(Pass1);

        PRESERVE_PASS(Pass1);
    public:
        MOCK_METHOD1(Run, bool (Plan*));  // NOLINT
    };

    class Pass3 : public Pass {
        RELY_PASS(Pass2);

        HOLD_BY_DEFAULT();
        PRESERVE_PASS(Pass1);
        PRESERVE_PASS(Pass2);
    public:
        MOCK_METHOD1(Run, bool (Plan*));  // NOLINT
    };
};

TEST_F(PreserveTest, InvalidateByDefault) {
    Pass1 pass1;
    Pass2 pass2;
    Pass3 pass3;

    {
        InSequence in_sequence;

        EXPECT_CALL(pass1, Run(NULL)).WillOnce(Return(true));
        EXPECT_CALL(pass2, Run(NULL)).WillOnce(Return(true));
        EXPECT_CALL(pass3, Run(NULL)).WillOnce(Return(true));
    }

    PassManager manager(NULL);
    manager.RegisterPass(&pass1);
    manager.RegisterPass(&pass2);
    manager.RegisterPass(&pass3);

    EXPECT_EQ(true, manager.Apply<Pass3>());
    EXPECT_EQ(true, manager.IsValid<Pass1>());
    EXPECT_EQ(true, manager.IsValid<Pass2>());
    EXPECT_EQ(true, manager.IsValid<Pass3>());
}

class RelyByTypeTest : public ::testing::Test {
public:
    class RobustScanPass : public Pass {
        HOLD_BY_DEFAULT();
    public:
        virtual bool Run(Plan* plan) {
            s_orders.push_back(0);
            return false;
        }
    };

    class FragileScanPass : public Pass {
        PRESERVE_BY_DEFAULT();
    public:
        virtual bool Run(Plan* plan) {
            s_orders.push_back(1);
            return false;
        }
    };

    class BasicTransformPass : public Pass {
        RELY_PASS(FragileScanPass);
        RELY_PASS(RobustScanPass);
    public:
        virtual bool Run(Plan* plan) {
            s_orders.push_back(2);
            return false;
        }
    };

    class EvilTransformPass : public Pass {
        RELY_PASS(BasicTransformPass);
        PRESERVE_PASS(FragileScanPass);
        INVALIDATE_PASS(RobustScanPass);
    public:
        virtual bool Run(Plan* plan) {
            s_orders.push_back(3);
            return true;
        }
    };

public:
    static std::vector<int> s_orders;
};

std::vector<int> RelyByTypeTest::s_orders;

TEST_F(RelyByTypeTest, Run) {
    s_orders.clear();

    PassManager manager(NULL);
    EXPECT_EQ(false, manager.IsValid<RobustScanPass>());
    EXPECT_EQ(false, manager.IsValid<FragileScanPass>());
    EXPECT_EQ(false, manager.IsValid<BasicTransformPass>());
    EXPECT_EQ(false, manager.IsValid<EvilTransformPass>());

    EXPECT_EQ(true, manager.Apply<EvilTransformPass>());
    EXPECT_THAT(s_orders, ElementsAre(0, 1, 2, 3));
    EXPECT_EQ(false, manager.IsValid<RobustScanPass>());
    EXPECT_EQ(true, manager.IsValid<FragileScanPass>());
    EXPECT_EQ(false, manager.IsValid<BasicTransformPass>());
    EXPECT_EQ(true, manager.IsValid<EvilTransformPass>());
}

class ApplyByTypeTest : public ::testing::Test {
public:
    class RobustScanPass : public Pass {
        HOLD_BY_DEFAULT();
    public:
        virtual bool Run(Plan* plan) {
            s_orders.push_back(0);
            return false;
        }
    };

    class FragileScanPass : public Pass {
        APPLY_PASS(BasicTransformPass);
    public:
        virtual bool Run(Plan* plan) {
            s_orders.push_back(1);
            return false;
        }
    };

    class BasicTransformPass : public Pass {
        RELY_PASS(RobustScanPass);
    public:
        virtual bool Run(Plan* plan) {
            s_orders.push_back(2);
            return false;
        }
    };

    class EvilTransformPass : public Pass {
        RELY_PASS(FragileScanPass);
    public:
        virtual bool Run(Plan* plan) {
            s_orders.push_back(3);
            return true;
        }
    };

public:
    static std::vector<int> s_orders;
};

std::vector<int> ApplyByTypeTest::s_orders;

TEST_F(ApplyByTypeTest, Run) {
    s_orders.clear();

    PassManager manager(NULL);
    EXPECT_EQ(true, manager.Apply<EvilTransformPass>());
    EXPECT_THAT(s_orders, ElementsAre(1, 0, 2, 3));
}

class ApplyRepeatedlyTest : public ::testing::Test {
public:
    class Pass1 : public Pass {
        HOLD_BY_DEFAULT();
    public:
        MOCK_METHOD1(Run, bool (Plan*));  // NOLINT
    };

    class Pass2A : public Pass {
        HOLD_BY_DEFAULT();
        PRESERVE_BY_DEFAULT();

        RELY_PASS(Pass1);
    public:
        MOCK_METHOD1(Run, bool (Plan*));  // NOLINT
    };

    class Pass2B : public Pass {
        HOLD_BY_DEFAULT();
        PRESERVE_BY_DEFAULT();

        RELY_PASS(Pass1);

        INVALIDATE_PASS(Pass2A);
    public:
        MOCK_METHOD1(Run, bool (Plan*));  // NOLINT
    };

    class Pass3A : public Pass {
        HOLD_BY_DEFAULT();
        PRESERVE_BY_DEFAULT();

        RELY_PASS(Pass2A);
        RELY_PASS(Pass2B);

        INVALIDATE_PASS(Pass2A);
    public:
        MOCK_METHOD1(Run, bool (Plan*));  // NOLINT
    };

    class Pass3B : public Pass {
        HOLD_BY_DEFAULT();
        PRESERVE_BY_DEFAULT();

        RELY_PASS(Pass2A);
        RELY_PASS(Pass2B);

        INVALIDATE_PASS(Pass3A);
    public:
        MOCK_METHOD1(Run, bool (Plan*));  // NOLINT
    };

    class Pass4 : public Pass {
        PRESERVE_BY_DEFAULT();

        RELY_PASS(Pass3A);
        RELY_PASS(Pass3B);

        INVALIDATE_PASS(Pass3A);
    public:
        MOCK_METHOD1(Run, bool (Plan*));  // NOLINT
    };

    class Pass5 : public Pass {
        RECURSIVE();
        PRESERVE_BY_DEFAULT();

        RELY_PASS(Pass3A);
        RELY_PASS(Pass3B);

        INVALIDATE_PASS(Pass3A);
    public:
        MOCK_METHOD1(Run, bool (Plan*));  // NOLINT
    };
};

TEST_F(ApplyRepeatedlyTest, ApplyRepeatedly) {
    Pass1 pass_1;
    Pass2A pass_2a;
    Pass2B pass_2b;
    Pass3A pass_3a;
    Pass3B pass_3b;
    Pass4 pass_4;

    {
        InSequence in_sequence;

        EXPECT_CALL(pass_1, Run(NULL)).WillOnce(Return(true));
        EXPECT_CALL(pass_2b, Run(NULL)).WillOnce(Return(true));
        EXPECT_CALL(pass_2a, Run(NULL)).WillOnce(Return(true));
        EXPECT_CALL(pass_3b, Run(NULL)).WillOnce(Return(true));
        EXPECT_CALL(pass_3a, Run(NULL)).WillOnce(Return(true));
        EXPECT_CALL(pass_4, Run(NULL)).WillOnce(Return(true));

        EXPECT_CALL(pass_2a, Run(NULL)).WillOnce(Return(true));
        EXPECT_CALL(pass_3a, Run(NULL)).WillOnce(Return(false));
        EXPECT_CALL(pass_4, Run(NULL)).WillOnce(Return(true));

        EXPECT_CALL(pass_3a, Run(NULL)).WillOnce(Return(false));
        EXPECT_CALL(pass_4, Run(NULL)).WillOnce(Return(false));
    }

    PassManager manager(NULL);
    manager.RegisterPass(&pass_1);
    manager.RegisterPass(&pass_2a);
    manager.RegisterPass(&pass_2b);
    manager.RegisterPass(&pass_3a);
    manager.RegisterPass(&pass_3b);
    manager.RegisterPass(&pass_4);

    manager.ApplyRepeatedly<Pass4>();
}

TEST_F(ApplyRepeatedlyTest, Apply) {
    Pass1 pass_1;
    Pass2A pass_2a;
    Pass2B pass_2b;
    Pass3A pass_3a;
    Pass3B pass_3b;
    Pass5 pass_5;

    {
        InSequence in_sequence;

        EXPECT_CALL(pass_1, Run(NULL)).WillOnce(Return(true));
        EXPECT_CALL(pass_2b, Run(NULL)).WillOnce(Return(true));
        EXPECT_CALL(pass_2a, Run(NULL)).WillOnce(Return(true));
        EXPECT_CALL(pass_3b, Run(NULL)).WillOnce(Return(true));
        EXPECT_CALL(pass_3a, Run(NULL)).WillOnce(Return(true));
        EXPECT_CALL(pass_5, Run(NULL)).WillOnce(Return(true));

        EXPECT_CALL(pass_2a, Run(NULL)).WillOnce(Return(true));
        EXPECT_CALL(pass_3a, Run(NULL)).WillOnce(Return(false));
        EXPECT_CALL(pass_5, Run(NULL)).WillOnce(Return(true));

        EXPECT_CALL(pass_3a, Run(NULL)).WillOnce(Return(false));
        EXPECT_CALL(pass_5, Run(NULL)).WillOnce(Return(false));
    }

    PassManager manager(NULL);
    manager.RegisterPass(&pass_1);
    manager.RegisterPass(&pass_2a);
    manager.RegisterPass(&pass_2b);
    manager.RegisterPass(&pass_3a);
    manager.RegisterPass(&pass_3b);
    manager.RegisterPass(&pass_5);

    manager.Apply<Pass5>();
}

}  // namespace planner
}  // namespace flume
}  // namespace baidu
