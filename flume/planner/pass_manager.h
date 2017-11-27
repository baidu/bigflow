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
//
// PassManager is capable of recording and driving the execution of different passes in
// one plan.

#ifndef FLUME_PLANNER_PASS_MANAGER_H_
#define FLUME_PLANNER_PASS_MANAGER_H_

#include <map>
#include <set>
#include <string>
#include <vector>

#include "glog/logging.h"

#include "flume/planner/common/tags.h"
#include "flume/util/reflection.h"

namespace baidu {
namespace flume {
namespace planner {

class Plan;
class Pass;
class DrawPlanPass;

// PassManager maintain pass relations. When a pass is applied in a plan, some kind of
// effect or property is imposed to that plan, which we called the result of that pass.
// When one other pass is applied to the same plan, the result of former pass may be
// disturbed or not. PassManager keeps those relationship in a global 'database'.
//
// There are five macros to declare relations between passes:
//
//      RELY_PASS(TARGET):          This pass relies on another pass, so before applying
//                                  this pass, PassManager will make sure the result of
//                                  TARGET is valid.
//
//      APPLY_PASS(TARGET):         This pass will apply another pass, no matter current
//                                  pass return true or false, PassManager will ensure
//                                  applying TARGET.
//
//      PRESERVE_BY_DEFAULT():      This pass will not disturb other passes, unless
//                                  explicitly decleared by INVALIDATE_PASS. Without
//                                  PRESERVE_BY_DEFAULT, a Pass will be treated as
//                                  disturbing all other Passes.
//
//      HOLD_BY_DEFAULT():          This pass will not be disturbed by other passes,
//                                  unless explicitly decleared by INVALIDATE_PASS.
//
//      PRESERVE_PASS(TARGET):      This pass will not disturd the result of TARGET.
//
//      INVALIDATE_PASS(TARGET):    This pass will disturb the result of TARGET.
//
//      RECURSIVE():                This pass will be recursively applied until unchanged.
//
//      ON_STAGE(STAGE1|STAGE2...)  This pass can only run on the specified stages.
//                                  If not set, the pass can ran on any stage.
//                                  Stages can be: LOGICAL_OPTIMIZING, TOPOLOGICAL_OPTIMIZING,
//                                  RUNTIME_OPTIMIZING, TRANSLATION_OPTIMIZING
//
// Examples below:
//
//      class FragilePass : public Pass {
//          PRESERVE_BY_DEFAULT();
//      public:
//          virtual bool Run(Plan* plan) {
//              return false;
//          }
//      };
//
//      class RobustPass : public Pass {
//          HOLD_BY_DEFAULT();
//          PRESERVE_BY_DEFAULT();
//          INVALIDATE_PASS(FragilePass);
//      public:
//          virtual bool Run(Plan* plan) {
//              return false;
//          }
//      };
//
//      class TransformPass : public Pass {
//          RELY_PASS(FragilePass);
//          RELY_PASS(RobustPass);
//      public:
//          virtual bool Run(Plan* plan) {
//              return false;
//          }
//      };
//
// Every instance of PassManager keep track of changes to its plan individually.
class PassManager {
public:
    template<class PassType>
    static std::string PassName();

    template<class TargetPass, class ThisPass>
    static void RegisterReliedPass(const ThisPass* pass);

    template<int32_t STAGE_MASK, class ThisPass>
    static void RegisterOnStage(const ThisPass* pass);

    template<class TargetPass, class ThisPass>
    static void RegisterApplyPass(const ThisPass* pass);

    template<class TargetPass, class ThisPass>
    static void RegisterInvalidatedPass(const ThisPass* pass);

    template<class TargetPass, class ThisPass>
    static void RegisterPreservedPass(const ThisPass* pass);

    template<class ThisPass>
    static void RegisterPreserveByDefault(const ThisPass* pass);

    template<class ThisPass>
    static void RegisterHoldByDefault(const ThisPass* pass);

    template<class ThisPass>
    static void RegisterRecursive(const ThisPass* pass);

    static void ShowPassRelations(std::ostream* stream);

public:
    explicit PassManager(Plan* plan);

    // Add pre-created pass, PassManager do not take its ownership.
    template<class PassType>
    void RegisterPass(PassType* pass);

    // Registered pass will be called every time when plan is changed.
    void SetDebugPass(DrawPlanPass* pass);

    // Run a pass. If an instance of this Pass has already been register, use the
    // registered instance, else use a new created pass.
    // Returns whatever the pass returns.
    template<class PassType>
    bool Apply();

    // Run a pass repeatedly, until the pass returns false.
    template<class PassType>
    void ApplyRepeatedly();

    // Return whether the result of given pass is valid.
    template<class PassType>
    bool IsValid();

    // Take PassType as valid. For debug only
    template<class PassType>
    void Validate();

    // Remove all apply relation. For test only
    template<class PassType>
    void UnregisterApplyPass();

private:
    // Pass relation facilities
    class RegisterBase;

    template<typename Register>
    class RegisterVariable;

    template<class ThisPass, class TargetPass>
    class ReliedPassRegister;

    template<class ThisPass, class TargetPass>
    class ApplyPassRegister;

    template<class ThisPass, class TargetPass>
    class InvalidatedPassRegister;

    template<class ThisPass, class TargetPass>
    class PreservedPassRegister;

    template<class ThisPass>
    class PreserveByDefaultRegister;

    template<class ThisPass>
    class HoldByDefaultRegister;

    template<class ThisPass>
    class RecursiveRegister;

    template<class ThisPass, int32_t STAGE_MASK>
    class OnStageRegister;

    typedef std::set<std::string> PassSet;
    typedef std::multimap<std::string, std::string> PassMap;

    static PassSet LookUp(const PassMap* map, const std::string& name);

    static PassSet Union(const PassSet& set1, const PassSet& set2);

    static PassSet Diff(const PassSet& set1, const PassSet& set2);

    static PassSet Intersect(const PassSet& set1, const PassSet& set2);

    static bool Includes(const PassSet& set1, const PassSet& set2);

    static PassMap* s_depend_map;
    static PassMap* s_apply_map;
    static PassMap* s_invalidate_map;
    static PassMap* s_preserve_map;

    static PassSet* s_white_passes;  // passes preserve other passes's result by default
    static PassSet* s_stable_passes;  // passes hold result against other passes
    static PassSet* s_recursive_passes;  // passes should be recursively applied

    static std::map<std::string, int32_t>* s_pass_stage;

private:
    static void InitPassRelations();

    void DeprecatedApplyRepeatedly(const std::string& pass);

    bool RunPasses(const PassSet& needed_passes);

    bool RecursiveRunPass(const std::string& pass);

    bool RunPass(const std::string& name);

    PassSet CollectReadyPasses(const PassSet& needed_passes);

    PassSet SelectSafePasses(const PassSet& passes);

    PassSet QueryPreservedPasses(const std::string& pass,
                                 const PassSet& total_passes);

private:
    Plan* m_plan;
    PassSet m_valid_passes;
    int m_steps;  // a changed apply of some pass is called one 'step'

    std::map<std::string, Pass*> m_external_passes;
    DrawPlanPass* m_debug_pass;

    // used for debug only
    std::string m_not_run_pass_name;
};

template<class PassType>
std::string PassManager::PassName() {
    return flume::Reflection<Pass>::TypeName<PassType>();
}

class PassManager::RegisterBase {
public:
    void Register() const {}
};

template<typename Register>
class PassManager::RegisterVariable {
public:
    static Register s_register;
};

template<typename Register>
Register PassManager::RegisterVariable<Register>::s_register;

#define _UNIQUE_PASS_FUNC_IMPL(name, number) name ## number
#define _UNIQUE_PASS_FUNC(name, number) _UNIQUE_PASS_FUNC_IMPL(name, number)

template<class ThisPass, class TargetPass>
class PassManager::ReliedPassRegister : public RegisterBase {
public:
    ReliedPassRegister() {
        InitPassRelations();
        s_depend_map->insert(std::make_pair(PassName<ThisPass>(), PassName<TargetPass>()));
    }
};

template<class TargetPass, class ThisPass>
void PassManager::RegisterReliedPass(const ThisPass* pass) {
    RegisterVariable<
        ReliedPassRegister<ThisPass, TargetPass>
    >::s_register.Register();
}

#define RELY_PASS(name) \
    virtual void _UNIQUE_PASS_FUNC(_RELY_PASS_, __LINE__)() { \
        PassManager::RegisterReliedPass<name>(this); \
    }

template<class ThisPass, int32_t STAGE_MASK>
class PassManager::OnStageRegister : public RegisterBase {
public:
    OnStageRegister() {
        InitPassRelations();
        s_pass_stage->insert(std::make_pair(PassName<ThisPass>(), STAGE_MASK));
    }
};

template<int32_t STAGE_MASK, class ThisPass>
void PassManager::RegisterOnStage(const ThisPass* pass) {
    RegisterVariable<
        OnStageRegister<ThisPass, STAGE_MASK>
    >::s_register.Register();
}

#define ON_STAGE(stage_mask) \
    virtual void _ON_STAGE() { \
        PassManager::RegisterOnStage<stage_mask>(this); \
    }

template<class ThisPass, class TargetPass>
class PassManager::ApplyPassRegister : public RegisterBase {
public:
    ApplyPassRegister() {
        InitPassRelations();
        s_apply_map->insert(std::make_pair(PassName<ThisPass>(), PassName<TargetPass>()));
    }
};

template<class TargetPass, class ThisPass>
void PassManager::RegisterApplyPass(const ThisPass* pass) {
    RegisterVariable<
        ApplyPassRegister<ThisPass, TargetPass>
    >::s_register.Register();
}

#define APPLY_PASS(name) \
    virtual void _UNIQUE_PASS_FUNC(_APPLY_PASS_, __LINE__)() { \
        PassManager::RegisterApplyPass<name>(this); \
    }

template<class ThisPass, class TargetPass>
class PassManager::InvalidatedPassRegister : public RegisterBase {
public:
    InvalidatedPassRegister() {
        InitPassRelations();
        s_invalidate_map->insert(std::make_pair(PassName<ThisPass>(), PassName<TargetPass>()));
    }
};

template<class TargetPass, class ThisPass>
void PassManager::RegisterInvalidatedPass(const ThisPass* pass) {
    RegisterVariable<
        InvalidatedPassRegister<ThisPass, TargetPass>
    >::s_register.Register();
}

#define INVALIDATE_PASS(name) \
    virtual void _UNIQUE_PASS_FUNC(_INVALIDATE_PASS_, __LINE__)() { \
        PassManager::RegisterInvalidatedPass<name>(this); \
    }

template<class ThisPass, class TargetPass>
class PassManager::PreservedPassRegister : public RegisterBase {
public:
    PreservedPassRegister() {
        InitPassRelations();
        s_preserve_map->insert(std::make_pair(PassName<ThisPass>(), PassName<TargetPass>()));
    }
};

template<class TargetPass, class ThisPass>
void PassManager::RegisterPreservedPass(const ThisPass* pass) {
    RegisterVariable<
        PreservedPassRegister<ThisPass, TargetPass>
    >::s_register.Register();
}

#define PRESERVE_PASS(name) \
    virtual void _UNIQUE_PASS_FUNC(_PRESERVE_PASS_, __LINE__)() { \
        PassManager::RegisterPreservedPass<name>(this); \
    }

template<class ThisPass>
class PassManager::PreserveByDefaultRegister : public RegisterBase {
public:
    PreserveByDefaultRegister() {
        InitPassRelations();
        s_white_passes->insert(PassName<ThisPass>());
    }
};

template<class ThisPass>
void PassManager::RegisterPreserveByDefault(const ThisPass* pass) {
    RegisterVariable<
        PreserveByDefaultRegister<ThisPass>
    >::s_register.Register();
}

#define PRESERVE_BY_DEFAULT() \
    virtual void _PRESERVE_BY_DEFAULT() { \
        PassManager::RegisterPreserveByDefault(this); \
    }

template<class ThisPass>
class PassManager::HoldByDefaultRegister : public RegisterBase {
public:
    HoldByDefaultRegister() {
        InitPassRelations();
        s_stable_passes->insert(PassName<ThisPass>());
    }
};

template<class ThisPass>
void PassManager::RegisterHoldByDefault(const ThisPass* pass) {
    RegisterVariable<
        HoldByDefaultRegister<ThisPass>
    >::s_register.Register();
}

#define HOLD_BY_DEFAULT() \
    virtual void _HOLD_BY_DEFAULT() { \
        PassManager::RegisterHoldByDefault(this); \
    }

template<class ThisPass>
class PassManager::RecursiveRegister : public RegisterBase {
public:
    RecursiveRegister() {
        InitPassRelations();
        s_recursive_passes->insert(PassName<ThisPass>());
    }
};

template<class ThisPass>
void PassManager::RegisterRecursive(const ThisPass* pass) {
    RegisterVariable<
        RecursiveRegister<ThisPass>
    >::s_register.Register();
}

#define RECURSIVE() \
    virtual void _RECURSIVE() { \
        PassManager::RegisterRecursive(this); \
    }

template<class PassType>
void PassManager::RegisterPass(PassType* pass) {
    m_external_passes[PassName<PassType>()] = pass;
}

template<class PassType>
bool PassManager::Apply() {
    PassSet passes;
    passes.insert(PassName<PassType>());
    return RunPasses(passes);
}

template<class PassType>
void PassManager::ApplyRepeatedly() {
    DeprecatedApplyRepeatedly(PassName<PassType>());
}

template<class PassType>
bool PassManager::IsValid() {
    return m_valid_passes.count(PassName<PassType>()) != 0;
}

template<class PassType>
void PassManager::Validate() {
    m_valid_passes.insert(PassName<PassType>());
    LOG(INFO) << "Validating pass " << PassName<PassType>();
}

template<class PassType>
void PassManager::UnregisterApplyPass() {
    m_not_run_pass_name = PassName<PassType>();
}

}  // namespace planner
}  // namespace flume
}  // namespace baidu

#endif  // FLUME_PLANNER_PASS_MANAGER_H_
