// Copyright (c) 2020 Computer Vision Center (CVC) at the Universitat Autonoma
// de Barcelona (UAB).
//
// This work is licensed under the terms of the MIT license.
// For a copy, see <https://opensource.org/licenses/MIT>.

#include <PxScene.h>
#include <cmath>
#include "Carla.h"
#include "Carla/Actor/ActorBlueprintFunctionLibrary.h"
#include "Carla/Sensor/RSSemanticLidar.h"

#include <compiler/disable-ue4-macros.h>
#include "carla/geom/Math.h"
#include <compiler/enable-ue4-macros.h>

#include "DrawDebugHelpers.h"
#include "Engine/CollisionProfile.h"
#include "Runtime/Engine/Classes/Kismet/KismetMathLibrary.h"
#include "Runtime/Core/Public/Async/ParallelFor.h"
#include "Carla/Sensor/csv_reader.hpp"

namespace crp = carla::rpc;

FActorDefinition ARSSemanticLidar::GetSensorDefinition()
{
  return UActorBlueprintFunctionLibrary::MakeLidarDefinition(TEXT("ray_cast_rs_semantic"));
}

ARSSemanticLidar::ARSSemanticLidar(const FObjectInitializer& ObjectInitializer)
  : Super(ObjectInitializer)
{
  PrimaryActorTick.bCanEverTick = true;
}

void ARSSemanticLidar::Set(const FActorDescription &ActorDescription)
{
  Super::Set(ActorDescription);
  FLidarDescription LidarDescription;
  UActorBlueprintFunctionLibrary::SetLidar(ActorDescription, LidarDescription);
  Set(LidarDescription);
}

void ARSSemanticLidar::Set(const FLidarDescription &LidarDescription)
{
  Description = LidarDescription;
  SemanticLidarData = FSemanticLidarData(Description.Channels);

  // SemanticLidarData(uint32_t ChannelCount = 0u)
  //     : _header(Index::SIZE + ChannelCount, 0u) {
  //     _header[Index::ChannelCount] = ChannelCount;
  //   }


  CreateLasers();// vectical angle 设置
  PointsPerChannel.resize(Description.Channels);//std::vector<uint32_t> PointsPerChannel;
}

void ARSSemanticLidar::CreateLasers()
{
  const auto NumberOfLasers = Description.Num_rs;
  // std::string file_name = static_cast<std::string>(Description.csv_file_name);
  std::vector<std::vector<double>> data_angle;
  if (!CsvReader::ReadCsvFile("/home/stephane/data.csv", data_angle)) {
        // ROS_INFO_STREAM("cannot get csv file!" << file_name << "will return !");
        return;
    }
  check(NumberOfLasers > 0u);
  // const float DeltaAngle = NumberOfLasers == 1u ? 0.f :
  //   (Description.UpperFovLimit - Description.LowerFovLimit) /
  //   static_cast<float>(NumberOfLasers - 1);
  
  
  LaserAngles.Empty(NumberOfLasers);
  LaserAngles_h.Empty(NumberOfLasers);
  Time_sim.Empty(NumberOfLasers);

  for (int i = 0u; i < data_angle.size(); ++i) {
        auto &data = data_angle[i];
        LaserAngles.Emplace(data[2]);
        LaserAngles_h.Emplace(data[1]);
        Time_sim.Emplace(data[0]);
    }
  // for(auto i = 0u; i < NumberOfLasers; ++i)
  // {
  //   const float VerticalAngle =
  //       Description.UpperFovLimit - static_cast<float>(i) * DeltaAngle;
  //   LaserAngles.Emplace(VerticalAngle);
  // }
}

void ARSSemanticLidar::PostPhysTick(UWorld *World, ELevelTick TickType, float DeltaTime)
{
  TRACE_CPUPROFILER_EVENT_SCOPE(ARSSemanticLidar::PostPhysTick);
  SimulateLidar(DeltaTime);

  {
    TRACE_CPUPROFILER_EVENT_SCOPE_STR("Send Stream");
    auto DataStream = GetDataStream(*this);
    DataStream.Send(*this, SemanticLidarData, DataStream.PopBufferFromPool());
  }
}

void ARSSemanticLidar::SimulateLidar(const float DeltaTime)//DeltaTime: 1/ (scan per second) =1帧的时间
{
  TRACE_CPUPROFILER_EVENT_SCOPE(ARSSemanticLidar::SimulateLidar);
  const uint32 ChannelCount = Description.Channels;
  // const uint32 PointsToScanWithOneLaser = // DeltaTime中有多少个scan,一个scan是32/64线(velodyne)
  //   FMath::RoundHalfFromZero(
  //       Description.PointsPerSecond * DeltaTime / float(ChannelCount));

  const uint32 PointsToScanWithOneLaser = FMath::RoundHalfFromZero( Description.Num_rs * DeltaTime * 10.0 );
  //Robosense输出: 10Hz 78750个点，PointsToScanWithOneLaser是DeltaTime输出的点数
  //FMath::RoundHalfFromZero 四舍五入


  // if (PointsToScanWithOneLaser <= 0)
  // {
  //   UE_LOG(
  //       LogCarla,
  //       Warning,
  //       TEXT("%s: no points requested this frame, try increasing the number of points per second."),
  //       *GetName());
  //   return;
  // }

  // check(ChannelCount == LaserAngles.Num());

  const uint32_t start_index = SemanticLidarData.Get_start_index();// HorizontalAngle这里用作记录点的索引, 用作下一次检索的起点(未读的)

  // const float AngleDistanceOfTick = Description.RotationFrequency * Description.HorizontalFov
  //     * DeltaTime;//DeltaTime中, 旋转的角度
  // const float AngleDistanceOfLaserMeasure = AngleDistanceOfTick / PointsToScanWithOneLaser;//DeltaTime中每一帧旋转的角度

  ResetRecordedHits(ChannelCount, PointsToScanWithOneLaser);//划分数组 1*delta_time中的产生点
  PreprocessRays(ChannelCount, PointsToScanWithOneLaser);   //划分数组 1*delta_time中的产生点

  GetWorld()->GetPhysicsScene()->GetPxScene()->lockRead();
  {
    TRACE_CPUPROFILER_EVENT_SCOPE(ParallelFor);
    uint32_t ind;
    ParallelFor(ChannelCount, [&](int32 idxChannel) { // unreal engine parallelfor
      TRACE_CPUPROFILER_EVENT_SCOPE(ParallelForTask);

      FCollisionQueryParams TraceParams = FCollisionQueryParams(FName(TEXT("Laser_Trace")), true, this);
      TraceParams.bTraceComplex = true;
      TraceParams.bReturnPhysicalMaterial = false;
      
      for (auto idxPtsOneLaser = 0u; idxPtsOneLaser < PointsToScanWithOneLaser; idxPtsOneLaser++) {
        FHitResult HitResult;
        ind = start_index+idxPtsOneLaser;//

        if(ind >= Description.Num_rs) {
          ind = ind % Description.Num_rs; // ind =[0,78749]
        }

        const float VertAngle  = LaserAngles[ind];
        const float HorizAngle = LaserAngles_h[ind];
        const bool PreprocessResult = RayPreprocessCondition[idxChannel][idxPtsOneLaser];

        if (PreprocessResult && ShootLaser(VertAngle, HorizAngle, HitResult, TraceParams)) {
          WritePointAsync(idxChannel, HitResult);
        }
      };
    });
  }
  GetWorld()->GetPhysicsScene()->GetPxScene()->unlockRead();

  FTransform ActorTransf = GetTransform();
  ComputeAndSaveDetections(ActorTransf);

  uint32_t end_index = start_index+PointsToScanWithOneLaser;

  if(end_index>=Description.Num_rs) {
    end_index = end_index % Description.Num_rs;
  }

  SemanticLidarData.Set_end_index(end_index);
}

void ARSSemanticLidar::ResetRecordedHits(uint32_t Channels, uint32_t MaxPointsPerChannel) {
  RecordedHits.resize(Channels);

  for (auto& hits : RecordedHits) {
    hits.clear();
    hits.reserve(MaxPointsPerChannel);//相同的vectical angle有多少个点
  }
}

void ARSSemanticLidar::PreprocessRays(uint32_t Channels, uint32_t MaxPointsPerChannel) {//反映Rays测量状况
  RayPreprocessCondition.resize(Channels);

  for (auto& conds : RayPreprocessCondition) {
    conds.clear();
    conds.resize(MaxPointsPerChannel);
    std::fill(conds.begin(), conds.end(), true);
  }
}

void ARSSemanticLidar::WritePointAsync(uint32_t channel, FHitResult &detection) {
	TRACE_CPUPROFILER_EVENT_SCOPE_STR(__FUNCTION__);
  DEBUG_ASSERT(GetChannelCount() > channel);
  RecordedHits[channel].emplace_back(detection);
}

void ARSSemanticLidar::ComputeAndSaveDetections(const FTransform& SensorTransform) {
	TRACE_CPUPROFILER_EVENT_SCOPE_STR(__FUNCTION__);
  for (auto idxChannel = 0u; idxChannel < Description.Channels; ++idxChannel)
    PointsPerChannel[idxChannel] = RecordedHits[idxChannel].size();
  SemanticLidarData.ResetMemory(PointsPerChannel);

  for (auto idxChannel = 0u; idxChannel < Description.Channels; ++idxChannel) {
    for (auto& hit : RecordedHits[idxChannel]) {
      FSemanticDetection detection;
      ComputeRawDetection(hit, SensorTransform, detection);
      SemanticLidarData.WritePointSync(detection);
    }
  }

  SemanticLidarData.WriteChannelCount(PointsPerChannel);
}

void ARSSemanticLidar::ComputeRawDetection(const FHitResult& HitInfo, const FTransform& SensorTransf, FSemanticDetection& Detection) const
{
    const FVector HitPoint = HitInfo.ImpactPoint;
    Detection.point = SensorTransf.Inverse().TransformPosition(HitPoint);
    //p^{sensor}=R^{from world to sensor}* p^{world}
    const FVector VecInc = - (HitPoint - SensorTransf.GetLocation()).GetSafeNormal();
    Detection.cos_inc_angle = FVector::DotProduct(VecInc, HitInfo.ImpactNormal);
    // n1*n2 =|n1|*|n2|*cos(sita)
    const FActorRegistry &Registry = GetEpisode().GetActorRegistry();

    const AActor* actor = HitInfo.Actor.Get();
    Detection.object_idx = 0;
    Detection.object_tag = static_cast<uint32_t>(HitInfo.Component->CustomDepthStencilValue);

    if (actor != nullptr) {

      const FCarlaActor* view = Registry.FindCarlaActor(actor);
      if(view)
        Detection.object_idx = view->GetActorId();

    }
    else {
      UE_LOG(LogCarla, Warning, TEXT("Actor not valid %p!!!!"), actor);
    }
}


bool ARSSemanticLidar::ShootLaser(const float VerticalAngle, const float HorizontalAngle, FHitResult& HitResult, FCollisionQueryParams& TraceParams) const
{
  TRACE_CPUPROFILER_EVENT_SCOPE_STR(__FUNCTION__);

  FHitResult HitInfo(ForceInit);

  FTransform ActorTransf = GetTransform();
  FVector LidarBodyLoc = ActorTransf.GetLocation();
  FRotator LidarBodyRot = ActorTransf.Rotator();

  FRotator LaserRot (VerticalAngle, HorizontalAngle, 0);  // float InPitch, float InYaw, float InRoll
  FRotator ResultRot = UKismetMathLibrary::ComposeRotators(
    LaserRot,
    LidarBodyRot
  );

  const auto Range = Description.Range;
  FVector EndTrace = Range * UKismetMathLibrary::GetForwardVector(ResultRot) + LidarBodyLoc;

  GetWorld()->ParallelLineTraceSingleByChannel(
    HitInfo,
    LidarBodyLoc,
    EndTrace,
    ECC_GameTraceChannel2,
    TraceParams,
    FCollisionResponseParams::DefaultResponseParam
  );

  if (HitInfo.bBlockingHit) {
    HitResult = HitInfo;
    return true;
  } else {
    return false;
  }
}
